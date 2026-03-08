import os, json, time
from datetime import datetime, timedelta, timezone
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
import urllib.request

CALENDAR_ID   = 'pfpdmjv83bu9v17ius7qgi33p0@group.calendar.google.com'
FIRESTORE_URL = 'https://firestore.googleapis.com/v1/projects/radiohdr-39922/databases/(default)/documents/emissions'
SCOPES        = ['https://www.googleapis.com/auth/calendar.readonly']

JOURS_FR = {
    0: 'lundi', 1: 'mardi', 2: 'mercredi',
    3: 'jeudi', 4: 'vendredi', 5: 'samedi', 6: 'dimanche'
}

def get_credentials():
    token_json = os.environ.get('GOOGLE_TOKEN_JSON')
    if not token_json:
        raise Exception('Variable GOOGLE_TOKEN_JSON manquante')
    creds = Credentials.from_authorized_user_info(json.loads(token_json), SCOPES)
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
    return creds

def get_firestore_token():
    """Obtenir un token Firebase via l'API REST (pas de SDK)."""
    # Utilise la clé API Firebase pour accès non authentifié en mode test
    return None

def fetch_existing_emissions():
    """Récupère les documents existants dans Firestore."""
    url = f'{FIRESTORE_URL}?pageSize=200'
    req = urllib.request.Request(url, headers={'Content-Type': 'application/json'})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
            docs = data.get('documents', [])
            # Retourne un dict {calendar_event_id: firestore_doc_name}
            result = {}
            for doc in docs:
                fields = doc.get('fields', {})
                cal_id = fields.get('calendarEventId', {}).get('stringValue', '')
                if cal_id:
                    result[cal_id] = doc['name']
            return result
    except Exception as e:
        print(f'[firestore] Erreur lecture: {e}')
        return {}

def upsert_emission(doc_id, data, existing_doc_name=None):
    """Crée ou met à jour une émission dans Firestore."""
    fields = {
        'titre':           {'stringValue': data.get('titre', '')},
        'description':     {'stringValue': data.get('description', '')},
        'jour':            {'stringValue': data.get('jour', '')},
        'heureDebut':      {'stringValue': data.get('heureDebut', '')},
        'heureFin':        {'stringValue': data.get('heureFin', '')},
        'calendarEventId': {'stringValue': data.get('calendarEventId', '')},
        'visible':         {'booleanValue': True},
    }
    # Préserver animateur et genre si doc existant (on ne les écrase pas)
    body = json.dumps({'fields': fields}).encode()

    if existing_doc_name:
        # PATCH = mise à jour partielle (préserve animateur/genre)
        url = f'https://firestore.googleapis.com/v1/{existing_doc_name}'
        url += '?updateMask.fieldPaths=titre&updateMask.fieldPaths=description'
        url += '&updateMask.fieldPaths=jour&updateMask.fieldPaths=heureDebut'
        url += '&updateMask.fieldPaths=heureFin&updateMask.fieldPaths=calendarEventId'
        method = 'PATCH'
    else:
        # POST = création avec ID auto
        url = f'{FIRESTORE_URL}?documentId={doc_id}'
        method = 'POST'

    req = urllib.request.Request(url, data=body, method=method,
                                  headers={'Content-Type': 'application/json'})
    try:
        with urllib.request.urlopen(req, timeout=10):
            pass
        print(f'[firestore] {"MAJ" if existing_doc_name else "CRÉÉ"}: {data["titre"]} ({data["jour"]} {data["heureDebut"]})')
    except Exception as e:
        print(f'[firestore] Erreur upsert: {e}')

def sync():
    print(f'[sync] Démarrage — {datetime.now().strftime("%Y-%m-%d %H:%M")}')
    creds   = get_credentials()
    service = build('calendar', 'v3', credentials=creds)

    # Fenêtre : aujourd'hui → J+7
    now      = datetime.now(timezone.utc)
    time_min = now.replace(hour=0, minute=0, second=0).isoformat()
    time_max = (now + timedelta(days=7)).replace(hour=23, minute=59, second=59).isoformat()

    print(f'[sync] Fenêtre: {time_min[:10]} → {time_max[:10]}')

    events_result = service.events().list(
        calendarId=CALENDAR_ID,
        timeMin=time_min,
        timeMax=time_max,
        singleEvents=True,  # développe les récurrents
        orderBy='startTime'
    ).execute()

    events = events_result.get('items', [])
    print(f'[sync] {len(events)} événement(s) trouvé(s)')

    existing = fetch_existing_emissions()

    synced = 0
    for event in events:
        title = event.get('summary', '').strip()
        if not title:
            continue

        start = event.get('start', {})
        end   = event.get('end', {})

        # Récupérer heure début/fin
        start_dt = start.get('dateTime') or start.get('date')
        end_dt   = end.get('dateTime')   or end.get('date')

        if not start_dt or not end_dt:
            continue

        # Parser les dates
        start_obj = datetime.fromisoformat(start_dt.replace('Z', '+00:00'))
        end_obj   = datetime.fromisoformat(end_dt.replace('Z', '+00:00'))

        # Convertir en heure locale France (UTC+1 ou UTC+2)
        # Simple : utiliser l'heure telle quelle si déjà avec timezone
        heure_debut = start_obj.strftime('%H:%M')
        heure_fin   = end_obj.strftime('%H:%M')
        jour        = JOURS_FR[start_obj.weekday()]

        description = event.get('description', '') or ''
        cal_event_id = event.get('id', '')

        # ID Firestore = basé sur l'ID Calendar (stable pour récurrents)
        # Pour les récurrents, l'id contient _ suivi de la date
        doc_id = cal_event_id.replace('_', '-')[:50]

        data = {
            'titre':           title,
            'description':     description.strip(),
            'jour':            jour,
            'heureDebut':      heure_debut,
            'heureFin':        heure_fin,
            'calendarEventId': cal_event_id,
        }

        existing_doc_name = existing.get(cal_event_id)
        upsert_emission(doc_id, data, existing_doc_name)
        synced += 1

    print(f'[sync] ✅ {synced} émission(s) synchronisée(s)')

def main():
    while True:
        try:
            sync()
        except Exception as e:
            print(f'[sync] Erreur: {e}')
        # Toutes les heures
        print('[sync] Prochain sync dans 1h...')
        time.sleep(3600)

if __name__ == '__main__':
    main()
