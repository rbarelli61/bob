"""
Orchestrator Duereti + Mistral + MySQL/MariaDB
Automatizza il recupero LETTURE/CURVE da Duereti con persistenza su DB.

Flusso:
  1. Token (10 min validità) → Export (con retry) → Polling (30+ min)
  2. Invoca Mistral SOLO per parsing/validazione input, mai per esecuzione diretta
  3. Estrae token/ticket/status da risposte JSON senza delega all'LLM
  4. Salva LETTURE e CURVE in tabelle MySQL/MariaDB separate

Uso:
  python duereti_orchestrator.py \
    --pod "IT001E..." \
    --df "12345678901" \
    --tipo LETTURE \
    --data-inizio 2024-01-01 \
    --data-fine 2024-01-31

Config file (YAML) supported. CLI args override config and environment variables.
"""

import asyncio
import csv
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

import requests
from pydantic import BaseModel, Field

try:
    import mysql.connector
    from mysql.connector import Error as MySQLError
    MYSQL_AVAILABLE = True
except ImportError:
    MYSQL_AVAILABLE = False
    MySQLError = Exception

try:
    import yaml
    YAML_AVAILABLE = True
except Exception:
    YAML_AVAILABLE = False

# ============================================================================
# CONFIGURAZIONE LOGGING
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("duereti_orchestrator.log"),
    ],
)
logger = logging.getLogger("DueretiOrchestrator")

# ============================================================================
# CONFIGURAZIONE DEFAULT
# ============================================================================

# Mistral/OpenWebUI
OPENWEBUI_URL = os.getenv("OPENWEBUI_URL", "http://localhost:8000").rstrip("/")
MISTRAL_MODEL = os.getenv("MISTRAL_MODEL", "mistral-small")
OPENWEBUI_API_KEY = os.getenv("OPENWEBUI_API_KEY", "")

# Duereti Tool (nel backend OpenWebUI)
DUERETI_TOOL_NAME = "duereti"

# MySQL/MariaDB
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_NAME = os.getenv("DB_NAME", "duereti")

# Polling
MAX_POLLING_ATTEMPTS = 120  # ~2 ore con 60s di intervallo
POLLING_INTERVAL_SECONDS = 60
TOKEN_VALIDITY_MINUTES = 10
REFRESH_TOKEN_BEFORE_EXPIRY_MINUTES = 1

# ============================================================================
# DATABASE MANAGER
# ============================================================================


class DatabaseManager:
    """Gestisce connessione e operazioni MySQL/MariaDB."""

    def __init__(self):
        self.connection = None
        self.db_host = DB_HOST
        self.db_port = DB_PORT
        self.db_user = DB_USER
        self.db_password = DB_PASSWORD
        self.db_name = DB_NAME

    def connect(self) -> bool:
        """Connetti a MySQL/MariaDB."""
        if not MYSQL_AVAILABLE:
            logger.warning("mysql.connector non disponibile. Installare: pip install mysql-connector-python")
            return False

        try:
            self.connection = mysql.connector.connect(
                host=self.db_host,
                port=self.db_port,
                user=self.db_user,
                password=self.db_password,
                database=self.db_name,
            )
            logger.info(f"Connesso a MySQL/MariaDB: {self.db_host}:{self.db_port}/{self.db_name}")
            return True
        except MySQLError as e:
            logger.error(f"Errore connessione DB: {e}")
            return False

    def disconnect(self):
        """Chiudi la connessione."""
        if self.connection and getattr(self.connection, 'is_connected', lambda: False)():
            try:
                self.connection.close()
            except Exception:
                pass
            logger.info("Disconnesso da MySQL/MariaDB")

    def create_tables(self) -> bool:
        """Crea le tabelle se non esistono."""
        if not self.connection or not getattr(self.connection, 'is_connected', lambda: False)():
            logger.error("Non connesso al database")
            return False

        cursor = self.connection.cursor()

        try:
            # Tabella LETTURE
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS duereti_letture (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    pod VARCHAR(50) NOT NULL,
                    df VARCHAR(20) NOT NULL,
                    data_lettura DATE NOT NULL,
                    ora_inizio TIME,
                    ora_fine TIME,
                    valore DECIMAL(12, 4),
                    unita_misura VARCHAR(10),
                    stato VARCHAR(50),
                    ticket VARCHAR(100),
                    data_importazione TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY unique_lettura (pod, df, data_lettura, ora_inizio),
                    KEY idx_pod (pod),
                    KEY idx_df (df),
                    KEY idx_data_lettura (data_lettura),
                    KEY idx_ticket (ticket)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
            """)
            logger.info("Tabella duereti_letture creata/verificata")

            # Tabella CURVE
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS duereti_curve (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    pod VARCHAR(50) NOT NULL,
                    df VARCHAR(20) NOT NULL,
                    data_curva DATE NOT NULL,
                    ora_inizio TIME,
                    ora_fine TIME,
                    valore DECIMAL(12, 4),
                    unita_misura VARCHAR(10),
                    stato VARCHAR(50),
                    ticket VARCHAR(100),
                    data_importazione TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY unique_curva (pod, df, data_curva, ora_inizio),
                    KEY idx_pod (pod),
                    KEY idx_df (df),
                    KEY idx_data_curva (data_curva),
                    KEY idx_ticket (ticket)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
            """)
            logger.info("Tabella duereti_curve creata/verificata")

            # Tabella log esportazioni
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS duereti_export_log (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    ticket VARCHAR(100) UNIQUE NOT NULL,
                    pod VARCHAR(50) NOT NULL,
                    df VARCHAR(20) NOT NULL,
                    tipo VARCHAR(20),
                    data_inizio DATE,
                    data_fine DATE,
                    status VARCHAR(50),
                    zip_path VARCHAR(500),
                    extracted_dir VARCHAR(500),
                    file_count INT,
                    data_richiesta TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    data_completamento TIMESTAMP NULL,
                    KEY idx_ticket (ticket),
                    KEY idx_pod (pod),
                    KEY idx_tipo (tipo),
                    KEY idx_data_richiesta (data_richiesta)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
            """)
            logger.info("Tabella duereti_export_log creata/verificata")

            self.connection.commit()
            return True

        except MySQLError as e:
            logger.error(f"Errore creazione tabelle: {e}")
            self.connection.rollback()
            return False
        finally:
            cursor.close()

    def log_export(
        self,
        ticket: str,
        pod: str,
        df: str,
        tipo: str,
        data_inizio: str,
        data_fine: str,
        status: str,
        zip_path: str = None,
        extracted_dir: str = None,
        file_count: int = 0,
    ) -> bool:
        """Registra l'esportazione nel log."""
        if not self.connection or not getattr(self.connection, 'is_connected', lambda: False)():
            logger.warning("Non connesso al DB, skip log export")
            return False

        cursor = self.connection.cursor()

        try:
            cursor.execute("""
                INSERT INTO duereti_export_log
                (ticket, pod, df, tipo, data_inizio, data_fine, status, zip_path, extracted_dir, file_count)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    status = VALUES(status),
                    zip_path = VALUES(zip_path),
                    extracted_dir = VALUES(extracted_dir),
                    file_count = VALUES(file_count),
                    data_completamento = IF(VALUES(status) = 'completed', NOW(), data_completamento)
            """, (
                ticket, pod, df, tipo, data_inizio, data_fine, status,
                zip_path, extracted_dir, file_count
            ))

            self.connection.commit()
            logger.info(f"Log export registrato: ticket={ticket}, status={status}")
            return True

        except MySQLError as e:
            logger.error(f"Errore log export: {e}")
            self.connection.rollback()
            return False
        finally:
            cursor.close()

    def insert_letture(self, records: list[dict]) -> int:
        """
        Inserisce record LETTURE nella tabella.
        Ogni record deve avere: pod, df, data_lettura, ora_inizio, ora_fine, valore, unita_misura, stato
        """
        if not self.connection or not getattr(self.connection, 'is_connected', lambda: False)():
            logger.warning("Non connesso al DB, skip insert letture")
            return 0

        if not records:
            return 0

        cursor = self.connection.cursor()
        inserted = 0

        try:
            for record in records:
                try:
                    cursor.execute("""
                        INSERT INTO duereti_letture
                        (pod, df, data_lettura, ora_inizio, ora_fine, valore, unita_misura, stato, ticket)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            valore = VALUES(valore),
                            stato = VALUES(stato),
                            data_importazione = NOW()
                    """, (
                        record.get("pod"),
                        record.get("df"),
                        record.get("data_lettura"),
                        record.get("ora_inizio"),
                        record.get("ora_fine"),
                        record.get("valore"),
                        record.get("unita_misura"),
                        record.get("stato"),
                        record.get("ticket"),
                    ))
                    inserted += 1
                except MySQLError as e:
                    logger.warning(f"Errore insert record: {record}, errore: {e}")
                    continue

            self.connection.commit()
            logger.info(f"Inseriti {inserted}/{len(records)} record LETTURE")
            return inserted

        except MySQLError as e:
            logger.error(f"Errore insert letture: {e}")
            self.connection.rollback()
            return 0
        finally:
            cursor.close()

    def insert_curve(self, records: list[dict]) -> int:
        """
        Inserisce record CURVE nella tabella.
        Struttura analoga a LETTURE.
        """
        if not self.connection or not getattr(self.connection, 'is_connected', lambda: False)():
            logger.warning("Non connesso al DB, skip insert curve")
            return 0

        if not records:
            return 0

        cursor = self.connection.cursor()
        inserted = 0

        try:
            for record in records:
                try:
                    cursor.execute("""
                        INSERT INTO duereti_curve
                        (pod, df, data_curva, ora_inizio, ora_fine, valore, unita_misura, stato, ticket)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            valore = VALUES(valore),
                            stato = VALUES(stato),
                            data_importazione = NOW()
                    """, (
                        record.get("pod"),
                        record.get("df"),
                        record.get("data_curva"),
                        record.get("ora_inizio"),
                        record.get("ora_fine"),
                        record.get("valore"),
                        record.get("unita_misura"),
                        record.get("stato"),
                        record.get("ticket"),
                    ))
                    inserted += 1
                except MySQLError as e:
                    logger.warning(f"Errore insert record: {record}, errore: {e}")
                    continue

            self.connection.commit()
            logger.info(f"Inseriti {inserted}/{len(records)} record CURVE")
            return inserted

        except MySQLError as e:
            logger.error(f"Errore insert curve: {e}")
            self.connection.rollback()
            return 0
        finally:
            cursor.close()


# ============================================================================
# PARSER CSV DUERETI
# ============================================================================


class DueretiCSVParser:
    """Parsa i file CSV estratti da Duereti."""

    @staticmethod
    def parse_letture_csv(csv_path: str, pod: str, df: str, ticket: str) -> list[dict]:
        """
        Parsa un file CSV LETTURE.
        Duereti produce CSV con struttura variabile; adattiamo il parser ai tuoi file.
        Struttura attesa: data | ora_inizio | ora_fine | valore | unita_misura | stato
        """
        records = []

        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f, delimiter=';')

                if not reader.fieldnames:
                    logger.warning(f"CSV vuoto o malformato: {csv_path}")
                    return records

                for row_num, row in enumerate(reader, start=2):
                    try:
                        # Adatta i nomi delle colonne ai tuoi dati
                        # Esempi: "Data", "Ora Inizio", "Ora Fine", "Valore", "Unità Misura", "Stato"
                        data_lettura = row.get("Data") or row.get("data")
                        ora_inizio = row.get("Ora Inizio") or row.get("ora_inizio")
                        ora_fine = row.get("Ora Fine") or row.get("ora_fine")
                        valore = row.get("Valore") or row.get("valore")
                        unita_misura = row.get("Unità Misura") or row.get("unita_misura") or "kWh"
                        stato = row.get("Stato") or row.get("stato") or "valido"

                        if not data_lettura:
                            logger.warning(f"Row {row_num}: data_lettura mancante, skip")
                            continue

                        records.append({
                            "pod": pod,
                            "df": df,
                            "data_lettura": data_lettura,
                            "ora_inizio": ora_inizio,
                            "ora_fine": ora_fine,
                            "valore": float(valore) if valore else None,
                            "unita_misura": unita_misura,
                            "stato": stato,
                            "ticket": ticket,
                        })

                    except Exception as e:
                        logger.warning(f"Row {row_num}: errore parsing, {e}")
                        continue

            logger.info(f"Parsed {len(records)} LETTURE da {csv_path}")
            return records

        except Exception as e:
            logger.error(f"Errore parsing LETTURE CSV {csv_path}: {e}")
            return records

    @staticmethod
    def parse_curve_csv(csv_path: str, pod: str, df: str, ticket: str) -> list[dict]:
        """
        Parsa un file CSV CURVE.
        Struttura analoga a LETTURE.
        """
        records = []

        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f, delimiter=';')

                if not reader.fieldnames:
                    logger.warning(f"CSV vuoto o malformato: {csv_path}")
                    return records

                for row_num, row in enumerate(reader, start=2):
                    try:
                        data_curva = row.get("Data") or row.get("data")
                        ora_inizio = row.get("Ora Inizio") or row.get("ora_inizio")
                        ora_fine = row.get("Ora Fine") or row.get("ora_fine")
                        valore = row.get("Valore") or row.get("valore")
                        unita_misura = row.get("Unità Misura") or row.get("unita_misura") or "kWh"
                        stato = row.get("Stato") or row.get("stato") or "valido"

                        if not data_curva:
                            logger.warning(f"Row {row_num}: data_curva mancante, skip")
                            continue

                        records.append({
                            "pod": pod,
                            "df": df,
                            "data_curva": data_curva,
                            "ora_inizio": ora_inizio,
                            "ora_fine": ora_fine,
                            "valore": float(valore) if valore else None,
                            "unita_misura": unita_misura,
                            "stato": stato,
                            "ticket": ticket,
                        })

                    except Exception as e:
                        logger.warning(f"Row {row_num}: errore parsing, {e}")
                        continue

            logger.info(f"Parsed {len(records)} CURVE da {csv_path}")
            return records

        except Exception as e:
            logger.error(f"Errore parsing CURVE CSV {csv_path}: {e}")
            return records


# ============================================================================
# UTILITY MISTRAL (parsing + direttive strict)
# ============================================================================


def _get_system_prompt_validation() -> str:
    """Prompt stretto per Mistral: SOLO parsing."""
    return """Tu sei un assistente per la validazione e il parsing di parametri Duereti.

REGOLE ASSOLUTE:
1. NON scrivere, generare né eseguire codice Python o comandi shell
2. NON richiamare funzioni, API o tool
3. SOLO parsing e validazione di input
4. Rispondi SEMPRE in JSON strutturato
5. Se la richiesta non è chiara, chiedi chiarimenti in JSON con "status": "unclear"

Task: Convalida i parametri e restituisci JSON con:
{
  "status": "valid" | "invalid" | "unclear",
  "pod": "...",
  "df": "...",
  "tipo": "LETTURE|CURVE",
  "data_inizio": "YYYY-MM-DD",
  "data_fine": "YYYY-MM-DD",
  "error": "..."  // solo se status=invalid
}

NON eseguire nulla. SOLO validare."""


def _call_mistral_validation(user_message: str) -> dict[str, Any]:
    """Chiama Mistral per validare i parametri input."""
    logger.info(f"Chiamata Mistral per validazione: {user_message[:100]}...")

    try:
        headers = {
            "Content-Type": "application/json",
        }
        if OPENWEBUI_API_KEY:
            # Supporta sia token puro che prefisso Bearer/Token fornito dall'utente
            headers["Authorization"] = OPENWEBUI_API_KEY if OPENWEBUI_API_KEY.startswith("Bearer ") or OPENWEBUI_API_KEY.startswith("Token ") else f"Bearer {OPENWEBUI_API_KEY}"

        payload = {
            "model": MISTRAL_MODEL,
            "messages": [
                {
                    "role": "system",
                    "content": _get_system_prompt_validation(),
                },
                {
                    "role": "user",
                    "content": user_message,
                },
            ],
            "temperature": 0.0,
            "max_tokens": 500,
        }

        response = requests.post(
            f"{OPENWEBUI_URL}/api/chat/completions",
            json=payload,
            headers=headers,
            timeout=30,
        )
        response.raise_for_status()

        result = response.json()
        assistant_message = result["choices"][0]["message"]["content"]

        logger.debug(f"Mistral response: {assistant_message}")

        try:
            parsed = json.loads(assistant_message)
        except json.JSONDecodeError:
            match = re.search(r"\{.*\}", assistant_message, re.DOTALL)
            if match:
                parsed = json.loads(match.group())
            else:
                logger.warning(f"Non posso parsare risposta Mistral: {assistant_message}")
                parsed = {"status": "error", "error": "Parse error"}

        return parsed

    except Exception as e:
        logger.error(f"Errore Mistral: {e}")
        return {"status": "error", "error": str(e)}


# ============================================================================
# UTILITY DUERETI TOOL
# ============================================================================


class DueretiToolInvoker:
    """Invoca i tool Duereti tramite OpenWebUI Backend API."""

    def __init__(self):
        self.base_url = OPENWEBUI_URL
        self.headers = {
            "Content-Type": "application/json",
        }
        if OPENWEBUI_API_KEY:
            self.headers["Authorization"] = OPENWEBUI_API_KEY if OPENWEBUI_API_KEY.startswith("Bearer ") or OPENWEBUI_API_KEY.startswith("Token ") else f"Bearer {OPENWEBUI_API_KEY}"

    def _call_tool(self, tool_name: str, tool_call_id: str, params: dict) -> str:
        """Invoca un tool Duereti tramite OpenWebUI."""
        logger.info(f"Invocazione tool: {tool_name} con params: {params}")

        try:
            payload = {
                "model": MISTRAL_MODEL,
                "messages": [
                    {
                        "role": "user",
                        "content": f"Chiama il tool {tool_name} con parametri: {json.dumps(params)}",
                    }
                ],
                "tools": [
                    {
                        "type": "function",
                        "function": {
                            "name": tool_name,
                            "description": f"Duereti {tool_name}",
                            "parameters": params,
                        },
                    }
                ],
            }

            response = requests.post(
                f"{self.base_url}/api/chat/completions",
                json=payload,
                headers=self.headers,
                timeout=600,
            )
            response.raise_for_status()

            result = response.json()
            logger.debug(f"Tool response: {result}")

            return json.dumps(result)

        except Exception as e:
            logger.error(f"Errore tool {tool_name}: {e}")
            raise


# ============================================================================
# ORCHESTRATOR PRINCIPALE
# ============================================================================


@dataclass
class DueretiRequest:
    """Parametri validati per una richiesta Duereti."""

    pod: str
    df: str
    tipo: str  # LETTURE | CURVE
    data_inizio: str  # YYYY-MM-DD
    data_fine: str  # YYYY-MM-DD


class DueretiOrchestrator:
    """Orchestrator con supporto DB."""

    def __init__(self, db_manager: DatabaseManager = None):
        self.invoker = DueretiToolInvoker()
        self.db = db_manager or DatabaseManager()
        self.token: Optional[str] = None
        self.token_received_at: Optional[datetime] = None
        self.current_ticket: Optional[str] = None

    def token_is_valid(self) -> bool:
        """Verifica se il token è ancora valido."""
        if not self.token or not self.token_received_at:
            return False

        expiry = self.token_received_at + timedelta(minutes=TOKEN_VALIDITY_MINUTES)
        refresh_before = expiry - timedelta(minutes=REFRESH_TOKEN_BEFORE_EXPIRY_MINUTES)

        return datetime.now() < refresh_before

    async def refresh_token_if_needed(self) -> bool:
        """Rinfresca il token se scadente."""
        if self.token_is_valid():
            logger.info("Token ancora valido.")
            return True

        logger.info("Token scaduto o non disponibile, richiedo nuovo token...")

        try:
            result_str = await asyncio.to_thread(
                self.invoker._call_tool,
                "duereti_request_token",
                "token_req_1",
                {},
            )

            result = json.loads(result_str)

            if result.get("status") == "token_received":
                self.token_received_at = datetime.now()
                logger.info(
                    f"Token ricevuto alle {self.token_received_at}, scade tra 10 min"
                )
                return True
            else:
                logger.error(f"Errore ricezione token: {result}")
                return False

        except Exception as e:
            logger.error(f"Errore refresh_token: {e}")
            return False

    async def request_export(self, req: DueretiRequest) -> Optional[str]:
        """Richiede l'export a Duereti."""
        if not await self.refresh_token_if_needed():
            logger.error("Impossibile ottenere token.")
            return None

        supply_points = [
            {
                "supplyPoint": req.pod,
                "df": req.df,
            }
        ]

        params = {
            "data_da": req.data_inizio,
            "data_a": req.data_fine,
            "mode": req.tipo,
            "supply_points_json": json.dumps(supply_points),
        }

        logger.info(f"Richiesta export: {params}")

        try:
            result_str = await asyncio.to_thread(
                self.invoker._call_tool,
                "duereti_request_export",
                "export_req_1",
                params,
            )

            result = json.loads(result_str)

            if result.get("status") == "export_requested":
                ticket = result.get("ticket")
                self.current_ticket = ticket
                logger.info(f"Export richiesto con ticket: {ticket}")

                # Log su DB
                if self.db.connection:
                    self.db.log_export(
                        ticket=ticket,
                        pod=req.pod,
                        df=req.df,
                        tipo=req.tipo,
                        data_inizio=req.data_inizio,
                        data_fine=req.data_fine,
                        status="requested",
                    )

                return ticket
            else:
                logger.error(f"Errore export: {result}")
                return None

        except Exception as e:
            logger.error(f"Errore request_export: {e}")
            return None

    async def poll_result(self, ticket: str, req: DueretiRequest, max_attempts: int = None) -> Optional[dict]:
        """Fa polling del risultato fino a completamento."""
        if max_attempts is None:
            max_attempts = MAX_POLLING_ATTEMPTS

        logger.info(
            f"Inizio polling ticket {ticket} (max {max_attempts} tentativi, "
            f"intervallo {POLLING_INTERVAL_SECONDS}s)"
        )

        for attempt in range(1, max_attempts + 1):
            if not await self.refresh_token_if_needed():
                logger.error("Token scaduto durante polling, impossibile rinnovare.")
                return None

            logger.info(
                f"[Tentativo {attempt}/{max_attempts}] "
                f"Verifica ticket {ticket}..."
            )

            try:
                result_str = await asyncio.to_thread(
                    self.invoker._call_tool,
                    "duereti_request_result",
                    f"result_req_{attempt}",
                    {"ticket": ticket},
                )

                result = json.loads(result_str)
                status = result.get("status")

                if status == "completed":
                    logger.info(f"✓ Risultato completato!")
                    logger.info(f"  ZIP: {result.get('zip_path')}")
                    logger.info(f"  Estratto in: {result.get('extracted_dir')}")
                    logger.info(f"  File: {result.get('files')}")

                    # Log completamento su DB
                    if self.db.connection:
                        self.db.log_export(
                            ticket=ticket,
                            pod=req.pod,
                            df=req.df,
                            tipo=req.tipo,
                            data_inizio=req.data_inizio,
                            data_fine=req.data_fine,
                            status="completed",
                            zip_path=result.get('zip_path'),
                            extracted_dir=result.get('extracted_dir'),
                            file_count=len(result.get('files', [])),
                        )

                    return result

                elif status == "not_ready" or status == "not_ready_or_not_found":
                    logger.info(
                        f"  Status: {status} - Attendo {POLLING_INTERVAL_SECONDS}s "
                        f"prima del prossimo tentativo..."
                    )
                    await asyncio.sleep(POLLING_INTERVAL_SECONDS)

                elif status == "token_expired_or_invalid":
                    logger.warning("Token scaduto durante polling, riprovo...")
                    self.token = None
                    self.token_received_at = None
                    await asyncio.sleep(5)

                else:
                    logger.error(f"Status sconosciuto: {status}")
                    logger.error(f"Risposta completa: {result}")
                    await asyncio.sleep(POLLING_INTERVAL_SECONDS)

            except Exception as e:
                logger.error(f"Errore polling (tentativo {attempt}): {e}")
                await asyncio.sleep(POLLING_INTERVAL_SECONDS)

        logger.error(f"Timeout polling ticket {ticket} dopo {max_attempts} tentativi")

        # Log timeout su DB
        if self.db.connection:
            self.db.log_export(
                ticket=ticket,
                pod=req.pod,
                df=req.df,
                tipo=req.tipo,
                data_inizio=req.data_inizio,
                data_fine=req.data_fine,
                status="timeout",
            )

        return None

    async def process_extracted_files(self, result: dict, req: DueretiRequest) -> int:
        """
        Processa i file estratti da Duereti e li salva nel DB.
        Ritorna il numero totale di record inseriti.
        """
        total_inserted = 0
        files = result.get('files', [])
        ticket = result.get('ticket', self.current_ticket)

        if not files:
            logger.warning("Nessun file estratto da processare")
            return 0

        parser = DueretiCSVParser()

        for file_path in files:
            file_path = Path(file_path)

            if not file_path.exists():
                logger.warning(f"File non trovato: {file_path}")
                continue

            logger.info(f"Processamento file: {file_path}")

            try:
                # Determina il tipo (LETTURE o CURVE) dal nome file o dal tipo richiesto
                if req.tipo == "LETTURE" or "lettur" in file_path.name.lower():
                    records = parser.parse_letture_csv(str(file_path), req.pod, req.df, ticket)
                    inserted = self.db.insert_letture(records)
                    total_inserted += inserted

                elif req.tipo == "CURVE" or "curv" in file_path.name.lower():
                    records = parser.parse_curve_csv(str(file_path), req.pod, req.df, ticket)
                    inserted = self.db.insert_curve(records)
                    total_inserted += inserted

                else:
                    logger.warning(f"Impossibile determinare il tipo di file: {file_path.name}")

            except Exception as e:
                logger.error(f"Errore processamento file {file_path}: {e}")
                continue

        logger.info(f"Totale record inseriti nel DB: {total_inserted}")
        return total_inserted

    async def run_full_flow(self, user_request: str) -> dict[str, Any]:
        """Flusso completo con salvataggio DB."""
        logger.info(f"Inizio orchestrazione: {user_request}")

        # Step 1: Validazione input via Mistral
        logger.info("Step 1/5: Validazione input con Mistral...")
        validation = _call_mistral_validation(user_request)

        if validation.get("status") != "valid":
            logger.error(f"Parametri non validi: {validation}")
            return {
                "status": "error",
                "error": "Parametri input non validi",
                "validation": validation,
            }

        # Crea DueretiRequest
        req = DueretiRequest(
            pod=validation["pod"],
            df=validation["df"],
            tipo=validation["tipo"],
            data_inizio=validation["data_inizio"],
            data_fine=validation["data_fine"],
        )

        logger.info(f"Parametri validati: POD={req.pod}, DF={req.df}, "
                   f"TIPO={req.tipo}, DATE={req.data_inizio} to {req.data_fine}")

        # Step 2: Connessione DB
        logger.info("Step 2/5: Connessione al database...")
        if MYSQL_AVAILABLE:
            if self.db.connect():
                self.db.create_tables()
            else:
                logger.warning("Impossibile connettere al DB, continuo senza persistenza")
        else:
            logger.warning("mysql.connector non disponibile, continuo senza DB")

        # Step 3: Richiesta export
        logger.info("Step 3/5: Richiesta export a Duereti...")
        ticket = await self.request_export(req)

        if not ticket:
            if self.db.connection:
                self.db.disconnect()
            return {
                "status": "error",
                "error": "Impossibile prenotare export",
            }

        # Step 4: Polling risultato
        logger.info("Step 4/5: Polling risultato...")
        result = await self.poll_result(ticket, req)

        if not result:
            if self.db.connection:
                self.db.disconnect()
            return {
                "status": "error",
                "error": "Timeout polling, risultato non disponibile",
                "ticket": ticket,
            }

        # Step 5: Processamento file e salvataggio DB
        logger.info("Step 5/5: Processamento file e salvataggio DB...")
        records_inserted = 0
        if self.db.connection:
            records_inserted = await self.process_extracted_files(result, req)

        if self.db.connection:
            self.db.disconnect()

        logger.info("Orchestrazione completata con successo")

        return {
            "status": "success",
            "ticket": ticket,
            "tipo": req.tipo,
            "data_inizio": req.data_inizio,
            "data_fine": req.data_fine,
            "pod": req.pod,
            "df": req.df,
            "zip_path": result.get("zip_path"),
            "extracted_dir": result.get("extracted_dir"),
            "files": result.get("files"),
            "records_inserted_db": records_inserted,
        }


# ============================================================================
# CLI
# ============================================================================


async def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Orchestrator automatico Duereti + Mistral + MySQL"
    )
    parser.add_argument("--pod", required=True, help="POD/PDR (es. IT001E...)")
    parser.add_argument("--df", required=True, help="Dato Fiscale (es. 12345678901)")
    parser.add_argument(
        "--tipo",
        choices=["LETTURE", "CURVE"],
        default="LETTURE",
        help="Tipo di dati",
    )
    parser.add_argument("--data-inizio", required=True, help="Data inizio (YYYY-MM-DD)")
    parser.add_argument("--data-fine", required=True, help="Data fine (YYYY-MM-DD)")
    parser.add_argument(
        "--openwebui-url",
        default="http://localhost:8000",
        help="URL OpenWebUI",
    )
    parser.add_argument(
        "--mistral-model",
        default="mistral-small",
        help="Nome modello Mistral",
    )
    parser.add_argument(
        "--openwebui-api-key",
        default=None,
        help="API key o token per autenticare verso OpenWebUI (override env)",
    )
    parser.add_argument(
        "--config",
        default=None,
        help="File di configurazione YAML (opzionale). I valori CLI sovrascrivono il file.",
    )
    parser.add_argument(
        "--db-host",
        default="localhost",
        help="MySQL host",
    )
    parser.add_argument(
        "--db-port",
        type=int,
        default=3306,
        help="MySQL port",
    )
    parser.add_argument(
        "--db-user",
        default="root",
        help="MySQL user",
    )
    parser.add_argument(
        "--db-password",
        default="",
        help="MySQL password",
    )
    parser.add_argument(
        "--db-name",
        default="duereti",
        help="MySQL database name",
    )

    args = parser.parse_args()

    # Load config file if provided
    config = {}
    if args.config:
        if not YAML_AVAILABLE:
            logger.error("PyYAML non installato: pip install pyyaml")
            print(json.dumps({"status": "error", "error": "PyYAML non installato"}, indent=2))
            return 2
        try:
            with open(args.config, 'r', encoding='utf-8') as cf:
                config = yaml.safe_load(cf) or {}
                logger.info(f"Configurazione caricata da {args.config}: {config}")
        except Exception as e:
            logger.error(f"Errore caricamento config {args.config}: {e}")
            print(json.dumps({"status": "error", "error": f"Errore caricamento config: {e}"}, indent=2))
            return 2

    # Override env/defaults with config, then CLI
    def pick(name, cli_val, config_key, env_val):
        if cli_val is not None:
            return cli_val
        if config_key and config.get(config_key) is not None:
            return config.get(config_key)
        return env_val

    globals()["OPENWEBUI_URL"] = pick("OPENWEBUI_URL", args.openwebui_url, "openwebui_url", os.getenv("OPENWEBUI_URL", OPENWEBUI_URL))
    globals()["MISTRAL_MODEL"] = pick("MISTRAL_MODEL", args.mistral_model, "mistral_model", os.getenv("MISTRAL_MODEL", MISTRAL_MODEL))
    globals()["OPENWEBUI_API_KEY"] = pick("OPENWEBUI_API_KEY", args.openwebui_api_key, "openwebui_api_key", os.getenv("OPENWEBUI_API_KEY", OPENWEBUI_API_KEY))
    globals()["DB_HOST"] = pick("DB_HOST", args.db_host, "db_host", os.getenv("DB_HOST", DB_HOST))
    globals()["DB_PORT"] = int(pick("DB_PORT", args.db_port, "db_port", os.getenv("DB_PORT", DB_PORT)))
    globals()["DB_USER"] = pick("DB_USER", args.db_user, "db_user", os.getenv("DB_USER", DB_USER))
    globals()["DB_PASSWORD"] = pick("DB_PASSWORD", args.db_password, "db_password", os.getenv("DB_PASSWORD", DB_PASSWORD))
    globals()["DB_NAME"] = pick("DB_NAME", args.db_name, "db_name", os.getenv("DB_NAME", DB_NAME))

    user_request = (
        f"Valida e recupera i dati Duereti: "
        f"POD={args.pod}, DF={args.df}, TIPO={args.tipo}, "
        f"data da {args.data_inizio} a {args.data_fine}"
    )

    db_manager = DatabaseManager()
    orchestrator = DueretiOrchestrator(db_manager=db_manager)
    result = await orchestrator.run_full_flow(user_request)

    logger.info(f"Risultato finale: {json.dumps(result, indent=2)}")
    print(json.dumps(result, indent=2))

    return 0 if result.get("status") == "success" else 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
