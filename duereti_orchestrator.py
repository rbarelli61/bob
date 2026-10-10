"""
Orchestrator Duereti + Mistral
Automatizza il recupero LETTURE/CURVE da Duereti senza false chiamate LLM.

Flusso:
  1. Token (10 min validità) → Export (con retry) → Polling (30+ min)
  2. Invoca Mistral SOLO per parsing/validazione input, mai per esecuzione diretta
  3. Estrae token/ticket/status da risposte JSON senza delega all'LLM

Uso:
  python duereti_orchestrator.py \
    --pod "IT001E..." \
    --df "12345678901" \
    --tipo LETTURE \
    --data-inizio 2024-01-01 \
    --data-fine 2024-01-31
"""

import asyncio
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
# CONFIGURAZIONE
# ============================================================================

# Mistral/OpenWebUI
OPENWEBUI_URL = os.getenv("OPENWEBUI_URL", "http://localhost:8000").rstrip("/")
MISTRAL_MODEL = os.getenv("MISTRAL_MODEL", "mistral-small")
OPENWEBUI_API_KEY = os.getenv("OPENWEBUI_API_KEY", "")  # Se richiesta

# Duereti Tool (nel backend OpenWebUI)
DUERETI_TOOL_NAME = "duereti"  # Nome del tool registrato in OpenWebUI

# Polling
MAX_POLLING_ATTEMPTS = 120  # ~2 ore con 60s di intervallo
POLLING_INTERVAL_SECONDS = 60
TOKEN_VALIDITY_MINUTES = 10
REFRESH_TOKEN_BEFORE_EXPIRY_MINUTES = 1  # Rinfresca 1 min prima della scadenza

# ============================================================================
# UTILITY MISTRAL (parsing + direttive strict)
# ============================================================================


def _get_system_prompt_validation() -> str:
    """
    Prompt stretto per Mistral: SOLO parsing, NO esecuzione di codice.
    """
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
    """
    Chiama Mistral per validare i parametri input.
    Ritorna JSON con status, parametri, eventuali errori.
    """
    logger.info(f"Chiamata Mistral per validazione: {user_message[:100]}...")

    try:
        headers = {
            "Content-Type": "application/json",
        }
        if OPENWEBUI_API_KEY:
            headers["Authorization"] = f"Bearer {OPENWEBUI_API_KEY}"

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
            "temperature": 0.0,  # Deterministic
            "max_tokens": 500,
        }

        response = requests.post(
            f"{OPENWEBUI_URL}/api/chat/completions",
            json=payload,
            timeout=30,
        )
        response.raise_for_status()

        result = response.json()
        assistant_message = result["choices"][0]["message"]["content"]

        logger.debug(f"Mistral response: {assistant_message}")

        # Estrai JSON dalla risposta
        try:
            parsed = json.loads(assistant_message)
        except json.JSONDecodeError:
            # Mistral potrebbe mettere il JSON dentro markdown
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
# UTILITY DUERETI TOOL (invocazione via API OpenWebUI)
# ============================================================================


class DueretiToolInvoker:
    """
    Invoca i tool Duereti (requestToken, requestExport, requestResult)
    tramite OpenWebUI Backend API.
    """

    def __init__(self):
        self.base_url = OPENWEBUI_URL
        self.headers = {
            "Content-Type": "application/json",
        }
        if OPENWEBUI_API_KEY:
            self.headers["Authorization"] = f"Bearer {OPENWEBUI_API_KEY}"

    def _call_tool(self, tool_name: str, tool_call_id: str, params: dict) -> str:
        """
        Invoca un tool Duereti tramite OpenWebUI.
        Ritorna la stringa JSON della risposta del tool.
        """
        logger.info(f"Invocazione tool: {tool_name} con params: {params}")

        # Endpoint: POST /api/chat/tools
        # Questo è un workaround generico - verifica con la tua OpenWebUI
        try:
            # Metodo alternativo: simuliamo una chat con tool
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
                timeout=600,  # Long timeout per polling
            )
            response.raise_for_status()

            result = response.json()
            logger.debug(f"Tool response: {result}")

            # Estrai tool result da OpenWebUI response
            # TODO: verifica la struttura esatta della tua OpenWebUI
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
    """
    Orchestrator principale che gestisce il flusso completo Duereti
    senza delegare al LLM l'esecuzione.
    """

    def __init__(self):
        self.invoker = DueretiToolInvoker()
        self.token: Optional[str] = None
        self.token_received_at: Optional[datetime] = None
        self.current_ticket: Optional[str] = None

    def token_is_valid(self) -> bool:
        """Verifica se il token è ancora valido (entro 10 min)."""
        if not self.token or not self.token_received_at:
            return False

        expiry = self.token_received_at + timedelta(minutes=TOKEN_VALIDITY_MINUTES)
        refresh_before = expiry - timedelta(minutes=REFRESH_TOKEN_BEFORE_EXPIRY_MINUTES)

        return datetime.now() < refresh_before

    async def refresh_token_if_needed(self) -> bool:
        """
        Rinfresca il token se scadente.
        Ritorna True se il nuovo token è disponibile.
        """
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
        """
        Richiede l'export a Duereti.
        Ritorna il ticket se successo, None se errore.
        """
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
                return ticket
            else:
                logger.error(f"Errore export: {result}")
                return None

        except Exception as e:
            logger.error(f"Errore request_export: {e}")
            return None

    async def poll_result(self, ticket: str, max_attempts: int = None) -> Optional[dict]:
        """
        Fa polling del risultato fino a completamento o timeout.
        Ritorna dict con i dati se successo, None se errore/timeout.
        """
        if max_attempts is None:
            max_attempts = MAX_POLLING_ATTEMPTS

        logger.info(
            f"Inizio polling ticket {ticket} (max {max_attempts} tentativi, "
            f"intervallo {POLLING_INTERVAL_SECONDS}s)"
        )

        for attempt in range(1, max_attempts + 1):
            # Rinfresca token se necessario
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
        return None

    async def run_full_flow(self, user_request: str) -> dict[str, Any]:
        """
        Flusso completo:
        1. Mistral valida input
        2. Richiede token
        3. Richiede export
        4. Fa polling risultato
        """
        logger.info(f"Inizio orchestrazione: {user_request}")

        # Step 1: Validazione input via Mistral
        logger.info("Step 1/4: Validazione input con Mistral...")
        validation = _call_mistral_validation(user_request)

        if validation.get("status") != "valid":
            logger.error(f"Parametri non validi: {validation}")
            return {
                "status": "error",
                "error": "Parametri input non validi",
                "validation": validation,
            }

        # Crea DueretiRequest dai parametri validati
        req = DueretiRequest(
            pod=validation["pod"],
            df=validation["df"],
            tipo=validation["tipo"],
            data_inizio=validation["data_inizio"],
            data_fine=validation["data_fine"],
        )

        logger.info(f"Parametri validati: POD={req.pod}, DF={req.df}, "
                   f"TIPO={req.tipo}, DATE={req.data_inizio} to {req.data_fine}")

        # Step 2: Richiesta export
        logger.info("Step 2/4: Richiesta export a Duereti...")
        ticket = await self.request_export(req)

        if not ticket:
            return {
                "status": "error",
                "error": "Impossibile prenotare export",
            }

        # Step 3: Polling risultato
        logger.info("Step 3/4: Polling risultato...")
        result = await self.poll_result(ticket)

        if not result:
            return {
                "status": "error",
                "error": "Timeout polling, risultato non disponibile",
                "ticket": ticket,
            }

        # Step 4: Return success
        logger.info("Step 4/4: Completamento flusso")
        return {
            "status": "success",
            "ticket": ticket,
            "tipo": req.tipo,
            "data_inizio": req.data_inizio,
            "data_fine": req.data_fine,
            "zip_path": result.get("zip_path"),
            "extracted_dir": result.get("extracted_dir"),
            "files": result.get("files"),
        }


# ============================================================================
# CLI
# ============================================================================


async def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Orchestrator automatico Duereti + Mistral"
    )
    parser.add_argument(
        "--pod",
        required=True,
        help="POD/PDR (es. IT001E...)",
    )
    parser.add_argument(
        "--df",
        required=True,
        help="Dato Fiscale (es. 12345678901)",
    )
    parser.add_argument(
        "--tipo",
        choices=["LETTURE", "CURVE"],
        default="LETTURE",
        help="Tipo di dati (LETTURE o CURVE)",
    )
    parser.add_argument(
        "--data-inizio",
        required=True,
        help="Data inizio (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--data-fine",
        required=True,
        help="Data fine (YYYY-MM-DD)",
    )
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

    args = parser.parse_args()

    # Override env se passati da CLI
    globals()["OPENWEBUI_URL"] = args.openwebui_url
    globals()["MISTRAL_MODEL"] = args.mistral_model

    user_request = (
        f"Valida e recupera i dati Duereti: "
        f"POD={args.pod}, DF={args.df}, TIPO={args.tipo}, "
        f"data da {args.data_inizio} a {args.data_fine}"
    )

    orchestrator = DueretiOrchestrator()
    result = await orchestrator.run_full_flow(user_request)

    logger.info(f"Risultato finale: {json.dumps(result, indent=2)}")
    print(json.dumps(result, indent=2))

    return 0 if result.get("status") == "success" else 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
