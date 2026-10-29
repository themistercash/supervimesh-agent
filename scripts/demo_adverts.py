# SPDX-License-Identifier: Apache-2.0
"""Démo étape 1 : affiche les annonces reçues par le boîtier companion.

Usage : python demo_adverts.py /dev/ttyUSB0
"""
import asyncio
import sys
from datetime import datetime, timezone

from meshcore import EventType, MeshCore


async def main(port: str) -> None:
    mc = await MeshCore.create_serial(port, auto_reconnect=True)
    if mc is None:
        sys.exit("Aucune réponse du boîtier sur " + port)
    await mc.ensure_contacts()

    async def on_advert(event) -> None:
        key = event.payload["public_key"]
        await mc.ensure_contacts(follow=True)
        c = mc.contacts.get(key, {})
        now = datetime.now(timezone.utc).strftime("%H:%M:%S")
        print(f"{now}  {key[:12]}  {c.get('adv_name', '?'):<20} type={c.get('type')} "
              f"pos=({c.get('adv_lat')}, {c.get('adv_lon')})")

    async def on_rx(event) -> None:
        p = event.payload
        print(f"   paquet entendu  snr={p.get('snr')}  rssi={p.get('rssi')}  chemin={p.get('path')}")

    mc.subscribe(EventType.ADVERTISEMENT, on_advert)
    mc.subscribe(EventType.RX_LOG_DATA, on_rx)
    print(f"À l'écoute sur {port} ({len(mc.contacts)} contacts connus). Ctrl+C pour quitter.")
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1] if len(sys.argv) > 1 else "/dev/ttyUSB0"))
