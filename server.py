"""
Serveur de signalisation WebRTC.

Rôle unique : mettre les joueurs d'un même "salon" (room) en relation au
moment de la connexion, en leur transmettant les messages techniques
(offres/réponses WebRTC, candidats ICE) nécessaires pour établir une
connexion directe entre eux.

Une fois cette connexion établie, ce serveur ne sert plus à rien pour la
partie en cours : tout le trafic de jeu passe directement entre les PC des
joueurs. Ce serveur reste donc très léger, il ne fait que relayer des
messages sans en comprendre le contenu.
"""

import asyncio
import json
import os

import websockets
from websockets import Headers, Response

rooms: dict[str, dict[int, "websockets.ServerConnection"]] = {}
room_next_id: dict[str, int] = {}


async def health_check(connection, request):
    # Render (et les services de "keep-alive") font des requêtes HTTP
    # classiques pour vérifier que le serveur répond. On les distingue
    # d'une vraie connexion WebSocket via l'en-tête "Upgrade".
    if request.headers.get("Upgrade", "").lower() != "websocket":
        return Response(
            200, "OK",
            Headers({"Content-Type": "text/plain"}),
            b"Signaling server is running.\n",
        )
    return None


async def handler(websocket):

    room_code: str | None = None
    peer_id: int | None = None

    try:
        async for raw_message in websocket:
            message = json.loads(raw_message)
            msg_type = message.get("type")

            if msg_type == "join":
                room_code = str(message["room"]).upper().strip()
                peer_id = room_next_id.get(room_code, 0)
                room_next_id[room_code] = peer_id + 1

                rooms.setdefault(room_code, {})

                # On confirme d'abord son propre identifiant au nouveau
                # venu — le client en a besoin avant de pouvoir traiter
                # quoi que ce soit d'autre.
                await websocket.send(json.dumps({
                    "type": "joined",
                    "id": peer_id,
                }))

                # Puis on lui annonce les joueurs déjà présents.
                for existing_id in rooms[room_code]:
                    await websocket.send(json.dumps({
                        "type": "peer_connected",
                        "id": existing_id,
                    }))

                # Et on annonce le nouveau venu à tout le monde déjà présent.
                for existing_ws in rooms[room_code].values():
                    await existing_ws.send(json.dumps({
                        "type": "peer_connected",
                        "id": peer_id,
                    }))

                rooms[room_code][peer_id] = websocket

            elif msg_type == "signal":
                # Relaie un message WebRTC (offer/answer/candidate) vers
                # le joueur destinataire, sans en interpréter le contenu.
                target_id = message["to"]
                if room_code and target_id in rooms.get(room_code, {}):
                    await rooms[room_code][target_id].send(json.dumps({
                        "type": "signal",
                        "from": peer_id,
                        "payload": message["payload"],
                    }))

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        if room_code and peer_id is not None and room_code in rooms:
            rooms[room_code].pop(peer_id, None)
            for remaining_ws in rooms[room_code].values():
                await remaining_ws.send(json.dumps({
                    "type": "peer_disconnected",
                    "id": peer_id,
                }))
            if not rooms[room_code]:
                del rooms[room_code]


async def main() -> None:
    # Render fournit le port à utiliser via la variable d'environnement PORT.
    port = int(os.environ.get("PORT", 10000))
    async with websockets.serve(handler, "0.0.0.0", port, process_request=health_check):
        print(f"[signaling] En écoute sur le port {port}.")
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
