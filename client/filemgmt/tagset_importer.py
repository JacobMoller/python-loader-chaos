"""
TagsetImporter — import de tagsets en masse depuis un fichier CSV ou JSON,
sans création de médias.

Formats supportés :

  CSV  (séparateur virgule ou point-virgule, avec ou sans en-tête) :
    name,type
    Day of week (number),numerical
    Day of week (string),alphanumerical
    ...

  JSON (liste de tagsets) :
    [
      {"name": "Day of week (number)", "type": "numerical"},
      {"name": "Day of week (string)", "type": "alphanumerical"},
      ...
    ]

Types acceptés (insensible à la casse) :
    alphanumerical  → 1
    timestamp       → 2
    time            → 3
    date            → 4
    numerical       → 5
"""

import csv
import json
import logging
from grpc import RpcError
import grpc_client

TYPE_MAP = {
    "alphanumerical": 1,
    "timestamp":      2,
    "time":           3,
    "date":           4,
    "numerical":      5,
}

# En-têtes reconnues comme ligne d'en-tête à ignorer
HEADER_ALIASES = {"name", "nom", "tagset", "type", "tagtype"}


class TagsetImporter:

    def __init__(self, grpc_host='localhost', grpc_port='50051'):
        self.client = grpc_client.LoaderClient(grpc_host=grpc_host, grpc_port=grpc_port)

    # ------------------------------------------------------------------ #
    #  Entrée publique                                                     #
    # ------------------------------------------------------------------ #

    def importFile(self, path: str) -> None:
        """Détecte le format depuis l'extension et lance l'import."""
        if path.lower().endswith('.json'):
            self._import_json(path)
        elif path.lower().endswith('.csv'):
            self._import_csv(path)
        else:
            print(f"Format non supporté : {path}. Utilisez .csv ou .json")

    # ------------------------------------------------------------------ #
    #  Import JSON                                                         #
    # ------------------------------------------------------------------ #

    def _import_json(self, path: str) -> None:
        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            print(f"Erreur lecture JSON : {e}")
            return

        if not isinstance(data, list):
            print("Erreur : le fichier JSON doit contenir une liste de tagsets.")
            return

        ok, skipped, errors = 0, 0, 0
        for i, item in enumerate(data):
            name = item.get('name', '').strip()
            type_str = item.get('type', '').strip().lower()

            if not name or not type_str:
                print(f"[{i+1}] Ignoré : champs 'name' ou 'type' manquants → {item}")
                skipped += 1
                continue

            type_id = TYPE_MAP.get(type_str)
            if type_id is None:
                print(f"[{i+1}] Ignoré : type inconnu '{type_str}' pour '{name}'")
                skipped += 1
                continue

            result = self._add_tagset(name, type_id, i + 1)
            if result:
                ok += 1
            else:
                errors += 1

        self._print_summary(ok, skipped, errors)

    # ------------------------------------------------------------------ #
    #  Import CSV                                                          #
    # ------------------------------------------------------------------ #

    def _import_csv(self, path: str) -> None:
        try:
            with open(path, 'r', encoding='utf-8') as f:
                # Détection automatique du séparateur (virgule ou point-virgule)
                sample = f.read(2048)
                f.seek(0)
                delimiter = ';' if sample.count(';') >= sample.count(',') else ','
                reader = csv.reader(f, delimiter=delimiter)

                ok, skipped, errors = 0, 0, 0
                for i, row in enumerate(reader):
                    # Ignore les lignes vides
                    if not any(cell.strip() for cell in row):
                        continue

                    # Récupère les colonnes utiles (nom et type)
                    # Supporte les deux formats :
                    #   format export M3  → col0=name, col1=(vide), col2=type_str
                    #   format standard   → col0=name, col1=type_str
                    name = row[0].strip().strip('"')
                    if len(row) >= 3 and row[2].strip():
                        type_str = row[2].strip().lower().strip('"')
                    elif len(row) >= 2 and row[1].strip():
                        type_str = row[1].strip().lower().strip('"')
                    else:
                        print(f"[ligne {i+1}] Ignorée : impossible de lire le type → {row}")
                        skipped += 1
                        continue

                    # Ignore les lignes d'en-tête
                    if name.lower() in HEADER_ALIASES or type_str in HEADER_ALIASES:
                        continue

                    # Résolution du type : string ou entier
                    if type_str.isdigit():
                        type_id = int(type_str)
                        if type_id not in TYPE_MAP.values():
                            print(f"[ligne {i+1}] Ignorée : type '{type_id}' hors plage [1-5] pour '{name}'")
                            skipped += 1
                            continue
                    else:
                        type_id = TYPE_MAP.get(type_str)
                        if type_id is None:
                            print(f"[ligne {i+1}] Ignorée : type inconnu '{type_str}' pour '{name}'")
                            skipped += 1
                            continue

                    result = self._add_tagset(name, type_id, i + 1)
                    if result:
                        ok += 1
                    else:
                        errors += 1

        except FileNotFoundError:
            print(f"Fichier introuvable : {path}")
            return

        self._print_summary(ok, skipped, errors)

    # ------------------------------------------------------------------ #
    #  Helpers                                                             #
    # ------------------------------------------------------------------ #

    def _add_tagset(self, name: str, type_id: int, line: int) -> bool:
        """Appelle le client gRPC et affiche le résultat. Retourne True si succès."""
        try:
            response = self.client.add_tagset(name, type_id)
            print(f"[ligne {line}] OK  [{type_id}] '{name}' → id={response.id}")
            return True
        except RpcError as e:
            print(f"[ligne {line}] ERR '{name}' : {e.details()}")
            return False

    @staticmethod
    def _print_summary(ok: int, skipped: int, errors: int) -> None:
        total = ok + skipped + errors
        print(f"\n── Résumé ──────────────────────────")
        print(f"  Total traité  : {total}")
        print(f"  Ajoutés       : {ok}")
        print(f"  Ignorés       : {skipped}")
        print(f"  Erreurs gRPC  : {errors}")
        print(f"────────────────────────────────────")
