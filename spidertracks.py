import asyncio
import json
import logging
import os
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
import pytak



def iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_time(s: str) -> datetime:
    # Expect ISO8601 UTC, typically ends with Z
    if s.endswith("Z"):
        return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)



def build_aff_body(cursor_iso: str, org_name: str) -> Dict[str, Any]:
    
    now = iso_utc(datetime.now(timezone.utc))
    return {
        "type": "dataRequest",
        "dataInfo": [
            {
                "affVer": "1.1",
                "provider": "spidertracks",
                "reqTime": now,
                "sysId": "spidertracks",
            }
        ],
        "msgRequest": [
            {
                "to": "spidertracks",
                "from": org_name,
                "msgType": "dataRequest",
                "dataCtrTime": cursor_iso,
            }
        ],
    }


async def aff_fetch(
    session: aiohttp.ClientSession,
    url: str,
    user: str,
    pw: str,
    org_name: str,
    cursor_iso: str,
    timeout_s: int = 15,
    user_agent: str = "Spidertracks-AFF-Adapter",
) -> Dict[str, Any]:
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": user_agent,
    }
    payload = build_aff_body(cursor_iso, org_name)
    timeout = aiohttp.ClientTimeout(total=timeout_s)

    async with session.post(
        url,
        auth=aiohttp.BasicAuth(user, pw),
        headers=headers,
        data=json.dumps(payload),
        timeout=timeout,
    ) as resp:
        text = await resp.text()
        if resp.status >= 300:
            raise RuntimeError(f"AFF HTTP {resp.status}: {text[:800]}")
        try:
            return json.loads(text)
        except Exception:
            raise RuntimeError(f"Could not parse AFF JSON. First 800 bytes: {text[:800]}")


def extract_features(doc: Dict[str, Any]) -> List[Dict[str, Any]]:
    feats = doc.get("features")
    if isinstance(feats, list):
        return feats
   
    for k in ("data", "result", "payload"):
        inner = doc.get(k)
        if isinstance(inner, dict) and isinstance(inner.get("features"), list):
            return inner["features"]
    return []


def sort_oldest_to_newest(features: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def k(f: Dict[str, Any]) -> str:
        p = (f or {}).get("properties", {}) or {}
        
        return str(p.get("posTime") or p.get("dataCtrTime") or "")
    return sorted(features, key=k)


def cursor_highest_dataCtrTime(features: List[Dict[str, Any]], fallback_cursor: str) -> str:
    best = parse_time(fallback_cursor)
    for f in features:
        p = (f or {}).get("properties", {}) or {}
        s = p.get("dataCtrTime")
        if not s:
            continue
        try:
            t = parse_time(s)
            if t > best:
                best = t
        except Exception:
            continue
    return iso_utc(best)



def cot_from_feature(
    feature: Dict[str, Any],
    cot_type: str,
    stale_window_seconds: int,
) -> Optional[bytes]:
    
    geom = (feature or {}).get("geometry", {}) or {}
    props = (feature or {}).get("properties", {}) or {}

    coords = geom.get("coordinates")
    if not (isinstance(coords, (list, tuple)) and len(coords) >= 2):
        return None

    lon = float(coords[0])
    lat = float(coords[1])
    hae = float(coords[2]) if len(coords) >= 3 and coords[2] is not None else 0.0

    pos_time = props.get("posTime")
    if not pos_time:
        return None

    
    now_dt = datetime.now(timezone.utc) - timedelta(seconds=5)
    now_iso = now_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    
    
    stale_dt = now_dt + timedelta(seconds=stale_window_seconds)
    stale_iso = stale_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    course = props.get("cog", 0)
    speed = props.get("spd", 0)

    callsign = props.get("unitId") or "UNKNOWN"
    stable_id = props.get("esn") or callsign
    uid = f"spider-{stable_id}"

    ce = props.get("hdop", 50)

    
    cot = (
        f'<event version="2.0" type="{cot_type}" uid="{uid}" '
        f'time="{now_iso}" start="{now_iso}" stale="{stale_iso}" how="m-g">'
        f'<point lat="{lat:.6f}" lon="{lon:.6f}" hae="{hae:.1f}" ce="{ce}" le="50"/>'
        f'<detail>'
        f'<contact callsign="{callsign}"/>'
        f'<track course="{float(course):.1f}" speed="{float(speed):.1f}"/>'
        f'<remarks>Spidertracks AFF</remarks>'
        f'</detail>'
        f'</event>'
    )
    return cot.encode("utf-8")



def build_pytak_cfg_from_env() -> Dict[str, Any]:
    
    cfg = dict(os.environ)

    
    if not cfg.get("PYTAK_TLS_CLIENT_PASSWORD") and cfg.get("PYTAK_TLS_CLIENT_CERT_PASSWORD"):
        cfg["PYTAK_TLS_CLIENT_PASSWORD"] = cfg["PYTAK_TLS_CLIENT_CERT_PASSWORD"]

    
    if not cfg.get("PYTAK_TLS_CLIENT_CAFILE") and cfg.get("PYTAK_TLS_CA"):
        cfg["PYTAK_TLS_CLIENT_CAFILE"] = cfg["PYTAK_TLS_CA"]

    return cfg


async def tak_send_batch(writer, messages: List[bytes]) -> int:
    
    if not messages:
        return 0

    try:
        for m in messages:
            writer.write(m + b"\n")
        
        await writer.drain()
        
        return len(messages)

    except Exception as e:
        
        logging.error("Send failed (socket broken): %s", e)
        raise e


async def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    
    aff_url = os.getenv("AFF_URL", "https://apigw.spidertracks.io/go/aff/feed")
    aff_user = os.environ["AFF_USER"]
    aff_pass = os.environ["AFF_PASS"]
    org_name = os.getenv("ORG_NAME", "Your Org Name")

    # TAK
    cot_url = os.environ["COT_URL"] 

    
    poll_s = int(os.getenv("POLL_SECONDS", "30"))  
    stale_s = int(os.getenv("STALE_WINDOW_SECONDS", "120"))
    cot_type = os.getenv("COT_TYPE", "a-f-A") 
    timeout_s = int(os.getenv("AFF_TIMEOUT_SECONDS", "15"))
    user_agent = os.getenv("USER_AGENT", "Spidertracks-AFF-Adapter")

    
    cursor = os.getenv(
        "AFF_CURSOR_ISO",
        iso_utc(datetime.now(timezone.utc) - timedelta(hours=1)),
    )

    cfg = build_pytak_cfg_from_env()

    logging.info("--- Environment Audit ---")
    logging.info("ORG Name:    %s", org_name)
    logging.info("AFF User:    %s", aff_user)
    logging.info("AFF URL:     %s", aff_url)
    logging.info("TAK Server:  %s", cot_url)
    logging.info("Poll (s):    %d", poll_s)
    logging.info("Stale (s):   %d", stale_s)
    logging.info("CoT type:    %s", cot_type)
    logging.info("Cursor seed: %s", cursor)
    logging.info("-------------------------")

    async with aiohttp.ClientSession() as session:
        
        reader, writer = None, None
        
        while True:
            try:
                
                if writer is None:
                    logging.info("Connecting to TAK Server...")
                    reader, writer = await pytak.protocol_factory(cfg)
                    logging.info("Connected!")

                
                total_points = 0
                total_sent = 0
        
      

                while True:
                    doc = await aff_fetch(
                        session=session,
                        url=aff_url,
                        user=aff_user,
                        pw=aff_pass,
                        org_name=org_name,
                        cursor_iso=cursor,
                        timeout_s=timeout_s,
                        user_agent=user_agent,
                    )

                    feats = extract_features(doc)
                    feats = sort_oldest_to_newest(feats)
                    count = len(feats)
                    total_points += count

                    
                    batch: List[bytes] = []
                    
                    
                    for f in feats:
                        # Use the stale window from env
                        cot = cot_from_feature(f, cot_type=cot_type, stale_window_seconds=stale_s)
                        if cot:
                            batch.append(cot)
                            # Optional: Log the first CoT of the batch for debugging
                            if len(batch) == 1:
                                try:
                                    logging.info("SAMPLE COT: %s", cot.decode("utf-8"))
                                except: pass

                    # 3. SEND BATCH using the PERSISTENT writer
                    sent = 0
                    if batch:
                        sent = await tak_send_batch(writer, batch)
                        
                    total_sent += sent

                    
                    if feats:
                        cursor = cursor_highest_dataCtrTime(feats, cursor)

                    logging.info("AFF page points=%d sent=%d next_cursor=%s", count, sent, cursor)

                    
                    if count < 1000:
                        break

                logging.info("AFF cycle points=%d sent=%d (sleep %ss)", total_points, total_sent, poll_s)
                await asyncio.sleep(poll_s)
                
            except Exception as e:
                logging.exception("Connection lost or loop error: %s", e)
                # If an error occurs, force a clean reconnection in the next loop
                if writer:
                    try:
                        writer.close()
                    except:
                        pass
                writer = None
                
                await asyncio.sleep(5)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Exiting.")



