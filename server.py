Serveur de signalisation WebRTC.

Rôle :

mettre les joueurs d'une même room en relation ;
relayer les messages WebRTC ;
empêcher de nouveaux joueurs de rejoindre une room
lorsque l'hôte a lancé la partie.

Le serveur Render ne transporte PAS l'audio ni le gameplay.
"""

import asyncio
import json
import os

import websockets
from websockets import Headers, Response

=============================================================
CONFIGURATION
=============================================================

MAX_PLAYERS = 4

=============================================================
ÉTAT DES ROOMS
=============================================================
room_code -> {peer_id: websocket}

rooms: dict[str, dict[int, "websockets.ServerConnection"]] = {}

room_code -> prochain ID suggéré

room_next_id: dict[str, int] = {}

Rooms dont l'accès est fermé.


Une room fermée peut toujours contenir les joueurs déjà présents,
mais aucun nouveau joueur ne peut la rejoindre.

closed_rooms: set[str] = set()

=============================================================
HEALTH CHECK
=============================================================

async def health_check(connection, request):
if request.headers.get("Upgrade", "").lower() != "websocket":
return Response(
200,
"OK",
Headers({
"Content-Type": "text/plain"
}),
b"Signaling server is running.\n",
)

return None
=============================================================
HANDLER PRINCIPAL
=============================================================

async def handler(websocket):

room_code: str | None = None
peer_id: int | None = None

try:

    async for raw_message in websocket:

        message = json.loads(raw_message)
        msg_type = message.get("type")

        # =================================================
        # JOIN
        # =================================================

        if msg_type == "join":

            room_code = str(
                message["room"]
            ).upper().strip()

            print(
                f"[signaling] Demande de connexion "
                f"à la room '{room_code}'."
            )

            # -------------------------------------------------
            # ROOM FERMÉE
            # -------------------------------------------------

            if room_code in closed_rooms:

                print(
                    f"[signaling] ❌ Room '{room_code}' "
                    f"fermée : connexion refusée."
                )

                await websocket.send(
                    json.dumps({
                        "type": "room_closed"
                    })
                )

                await websocket.close()

                return

            # -------------------------------------------------
            # CRÉER LA ROOM SI ELLE N'EXISTE PAS
            # -------------------------------------------------

            rooms.setdefault(
                room_code,
                {}
            )

            current_players = rooms[room_code]

            # -------------------------------------------------
            # ROOM PLEINE
            # -------------------------------------------------

            if len(current_players) >= MAX_PLAYERS:

                print(
                    f"[signaling] ❌ Room '{room_code}' "
                    f"pleine."
                )

                await websocket.send(
                    json.dumps({
                        "type": "room_full",
                        "max_players": MAX_PLAYERS
                    })
                )

                await websocket.close()

                return

            # -------------------------------------------------
            # TROUVER UN ID LIBRE ENTRE 1 ET 4
            # -------------------------------------------------

            peer_id = 0

            for possible_id in range(
                1,
                MAX_PLAYERS + 1
            ):

                if possible_id not in current_players:

                    peer_id = possible_id
                    break

            if peer_id == 0:

                print(
                    f"[signaling] ❌ Impossible de trouver "
                    f"un ID libre pour '{room_code}'."
                )

                await websocket.send(
                    json.dumps({
                        "type": "room_full",
                        "max_players": MAX_PLAYERS
                    })
                )

                await websocket.close()

                return

            room_next_id[room_code] = peer_id + 1

            # -------------------------------------------------
            # CONFIRMER LE JOIN AU NOUVEAU JOUEUR
            # -------------------------------------------------

            await websocket.send(
                json.dumps({
                    "type": "joined",
                    "id": peer_id,
                })
            )

            # -------------------------------------------------
            # DIRE AU NOUVEAU JOUEUR QUI EST DÉJÀ LÀ
            # -------------------------------------------------

            for existing_id in current_players:

                await websocket.send(
                    json.dumps({
                        "type": "peer_connected",
                        "id": existing_id,
                    })
                )

            # -------------------------------------------------
            # DIRE AUX JOUEURS EXISTANTS QUE LE NOUVEAU
            # JOUEUR EST ARRIVÉ
            # -------------------------------------------------

            for existing_ws in list(
                current_players.values()
            ):

                try:

                    await existing_ws.send(
                        json.dumps({
                            "type": "peer_connected",
                            "id": peer_id,
                        })
                    )

                except Exception as error:

                    print(
                        "[signaling] ⚠️ Erreur pendant "
                        f"peer_connected : {error}"
                    )

            # -------------------------------------------------
            # AJOUTER LE JOUEUR À LA ROOM
            # -------------------------------------------------

            current_players[peer_id] = websocket

            print(
                f"[signaling] ✅ Joueur {peer_id} "
                f"a rejoint '{room_code}' "
                f"({len(current_players)}/{MAX_PLAYERS})."
            )

        # =================================================
        # CLOSE ROOM
        # =================================================

        elif msg_type == "close_room":

            # -------------------------------------------------
            # VÉRIFICATIONS
            # -------------------------------------------------

            if room_code is None:

                print(
                    "[signaling] ❌ close_room reçu "
                    "sans room."
                )

                continue

            if peer_id is None:

                print(
                    "[signaling] ❌ close_room reçu "
                    "sans peer_id."
                )

                continue

            # -------------------------------------------------
            # SEUL L'HÔTE PEUT FERMER LA ROOM
            # -------------------------------------------------
            #
            # Dans ton système, l'hôte reçoit toujours l'ID 1.
            #

            if peer_id != 1:

                print(
                    f"[signaling] ❌ Le joueur {peer_id} "
                    f"a essayé de fermer '{room_code}'. "
                    f"Seul le joueur 1 peut le faire."
                )

                continue

            # -------------------------------------------------
            # DÉJÀ FERMÉE
            # -------------------------------------------------

            if room_code in closed_rooms:

                print(
                    f"[signaling] Room '{room_code}' "
                    f"déjà fermée."
                )

                continue

            # -------------------------------------------------
            # FERMETURE
            # -------------------------------------------------

            closed_rooms.add(
                room_code
            )

            print(
                f"[signaling] 🔒 Room '{room_code}' "
                f"fermée par l'hôte."
            )

            # -------------------------------------------------
            # PRÉVENIR TOUS LES JOUEURS PRÉSENTS
            # -------------------------------------------------

            players = rooms.get(
                room_code,
                {}
            )

            for existing_ws in list(
                players.values()
            ):

                try:

                    await existing_ws.send(
                        json.dumps({
                            "type": "room_closed"
                        })
                    )

                except Exception as error:

                    print(
                        "[signaling] ⚠️ Impossible "
                        "d'envoyer room_closed : "
                        f"{error}"
                    )

        # =================================================
        # SIGNAL WEBRTC
        # =================================================

        elif msg_type == "signal":

            if room_code is None:
                continue

            if peer_id is None:
                continue

            target_id = int(
                message["to"]
            )

            target_ws = rooms.get(
                room_code,
                {}
            ).get(
                target_id
            )

            if target_ws is None:
                continue

            await target_ws.send(
                json.dumps({
                    "type": "signal",
                    "from": peer_id,
                    "payload": message["payload"],
                })
            )

except websockets.exceptions.ConnectionClosed:

    pass

except Exception as error:

    print(
        f"[signaling] ⚠️ Erreur dans le handler : {error}"
    )

finally:

    # =================================================
    # DÉCONNEXION DU JOUEUR
    # =================================================

    if (
        room_code is not None
        and peer_id is not None
        and room_code in rooms
    ):

        rooms[room_code].pop(
            peer_id,
            None
        )

        print(
            f"[signaling] Joueur {peer_id} "
            f"a quitté '{room_code}'."
        )

        # -------------------------------------------------
        # PRÉVENIR LES JOUEURS RESTANTS
        # -------------------------------------------------

        for remaining_ws in list(
            rooms[room_code].values()
        ):

            try:

                await remaining_ws.send(
                    json.dumps({
                        "type": "peer_disconnected",
                        "id": peer_id,
                    })
                )

            except Exception:

                pass

        # -------------------------------------------------
        # ROOM VIDE
        # -------------------------------------------------

        if not rooms[room_code]:

            del rooms[room_code]

            room_next_id.pop(
                room_code,
                None
            )

            # Une room fermée n'a plus besoin
            # d'être conservée une fois vide.
            closed_rooms.discard(
                room_code
            )

            print(
                f"[signaling] 🗑️ Room '{room_code}' "
                f"supprimée."
            )
=============================================================
MAIN
=============================================================

async def main() -> None:

port = int(
    os.environ.get(
        "PORT",
        10000
    )
)

async with websockets.serve(
    handler,
    "0.0.0.0",
    port,
    process_request=health_check,
):

    print(
        f"[signaling] En écoute sur le port {port}."
    )

    await asyncio.Future()
=============================================================
LANCEMENT
=============================================================

if name == "main":

asyncio.run(main())
