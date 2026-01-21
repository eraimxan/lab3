    import threading
    import time
    import random
    import requests
    import sys
    from flask import Flask, request, jsonify

    app = Flask(__name__)

    # Node configuration
    node_id = None
    port = None
    peers = []  

    # Raft state
    state = 'Follower'
    current_term = 0
    voted_for = None
    log = []  
    commit_index = 0
    election_timeout = random.uniform(2.0, 3.0)  # УВЕЛИЧИЛ для AWS
    last_heartbeat = time.time()
    last_applied = 0   
    # For leader
    next_index = {}  
    match_index = {}  

    # Объявляем глобальные переменные для использования в функциях
    votes_received = 0

    def reset_election_timeout():
        global last_heartbeat, election_timeout
        election_timeout = random.uniform(2.0, 3.0)  # УВЕЛИЧИЛ
        last_heartbeat = time.time()
        print(f"[{node_id}] Reset timeout to {election_timeout:.2f}s")

    def election_timer():
        while True:
            time.sleep(0.1)
            if state != 'Leader' and time.time() - last_heartbeat > election_timeout:
                print(f"\n[{node_id}] ELECTION TIMEOUT after {time.time() - last_heartbeat:.2f}s")
                start_election()

    def start_election():
        global state, current_term, voted_for, votes_received
        state = 'Candidate'
        current_term += 1
        voted_for = node_id
        votes_received = 1  # голос за себя

        print(f"[{node_id}] Starting election for term {current_term}")

        # Запрашиваем голоса у всех пиров
        threads = []
        for peer in peers:
            thread = threading.Thread(target=request_vote_from_peer, args=(peer, current_term))
            threads.append(thread)
            thread.start()
        
        # Ждем немного результатов
        time.sleep(1.5)
        
        # Проверяем majority
        total_nodes = len(peers) + 1
        majority = total_nodes // 2 + 1  # Для 3 нодов: 3//2 + 1 = 1 + 1 = 2 голоса
        
        print(f"[{node_id}] Election results: {votes_received}/{total_nodes} votes, need {majority}")
        
        if votes_received >= majority:
            become_leader()
        else:
            # Не получили majority - возвращаемся в follower
            state = 'Follower'
            print(f"[{node_id}] Lost election, returning to follower")
            # Ждем перед следующей попыткой
            time.sleep(random.uniform(1.0, 2.0))

    def request_vote_from_peer(peer_url, term):
        global votes_received
        try:
            print(f"[{node_id}] Requesting vote from {peer_url}")
            response = requests.post(f"{peer_url}/request_vote", json={
                'term': term,
                'candidateId': node_id
            }, timeout=2.0)
            
            if response.status_code == 200:
                data = response.json()
                if data['voteGranted']:
                    votes_received += 1
                    print(f"[{node_id}] ✓ Got vote from {peer_url}")
                else:
                    print(f"[{node_id}] ✗ Vote denied by {peer_url}, term: {data.get('term')}")
            else:
                print(f"[{node_id}] ✗ Bad response from {peer_url}: {response.status_code}")
        except Exception as e:
            print(f"[{node_id}] ✗ Error contacting {peer_url}: {e}")

    def become_leader():
        global state
        state = 'Leader'
        print(f"\n{'='*50}")
        print(f"[{node_id}] ====== BECAME LEADER for term {current_term} ======")
        print(f"{'='*50}\n")
        
        for peer in peers:
            next_index[peer] = len(log) + 1
            match_index[peer] = 0
        
        # Запускаем heartbeats
        threading.Thread(target=send_heartbeats, daemon=True).start()

    def send_heartbeats():
        while state == 'Leader':
            for peer in peers:
                try:
                    requests.post(f"{peer}/append_entries", json={
                        'term': current_term,
                        'leaderId': node_id,
                        'entries': []
                    }, timeout=0.5)
                except:
                    pass
            time.sleep(1.0)  # Heartbeat каждую секунду

    def send_append_entries(peer, entries):
        try:
            response = requests.post(f"{peer}/append_entries", json={
                'term': current_term,
                'leaderId': node_id,
                'entries': entries
            }, timeout=1.0)
            
            if response.status_code == 200:
                data = response.json()
                if data['success']:
                    if entries:
                        print(f"[{node_id}] Append success to {peer}")
                        match_index[peer] = len(log) 
                        check_commit()
                else:
                    # Term mismatch, step down
                    if data['term'] > current_term:
                        become_follower(data['term'])
        except Exception as e:
            print(f"[{node_id}] Failed to send to {peer}: {e}")

    def check_commit():
        global commit_index, last_applied
        if state != 'Leader':
            return

        total_nodes = len(peers) + 1
        majority = total_nodes // 2 + 1

        for N in range(commit_index + 1, len(log) + 1):
            count = 1  # себя
            for peer in peers:
                if match_index.get(peer, 0) >= N:
                    count += 1

            if count >= majority and log[N-1]['term'] == current_term:
                commit_index = N
                print(f"[{node_id}] Entry committed (index={commit_index}, term={current_term})")

        if commit_index > last_applied:
            for i in range(last_applied, commit_index):
                print(f"[{node_id}] Applied: {log[i]['command']} (index={i+1})")
            last_applied = commit_index

    def become_follower(term):
        global state, current_term, voted_for
        print(f"[{node_id}] Stepping down to follower, new term {term}")
        state = 'Follower'
        current_term = term
        voted_for = None
        reset_election_timeout()

    # API Endpoints
    @app.route('/request_vote', methods=['POST'])
    def request_vote():
        global voted_for, current_term, state

        data = request.json
        term = data['term']
        candidate_id = data['candidateId']

        print(f"[{node_id}] Received vote request from {candidate_id} for term {term}")

        if term < current_term:
            return jsonify({'term': current_term, 'voteGranted': False})

        if term > current_term:
            # Новый терм - сбрасываем состояние
            current_term = term
            state = 'Follower'
            voted_for = None
            reset_election_timeout()

        if voted_for is None or voted_for == candidate_id:
            voted_for = candidate_id
            reset_election_timeout()
            print(f"[{node_id}] ✓ Voting for {candidate_id} in term {term}")
            return jsonify({'term': current_term, 'voteGranted': True})

        print(f"[{node_id}] ✗ Already voted for {voted_for} in term {current_term}")
        return jsonify({'term': current_term, 'voteGranted': False})

    @app.route('/append_entries', methods=['POST'])
    def append_entries():
        global current_term, state, voted_for, log, commit_index
        data = request.json
        term = data['term']
        leader_id = data['leaderId']
        entries = data.get('entries', [])

        print(f"[{node_id}] Received heartbeat from {leader_id}, term {term}")

        if term < current_term:
            return jsonify({'term': current_term, 'success': False})

        if term > current_term:
            # Обновляем терм и становимся follower
            current_term = term
            state = 'Follower'
            voted_for = None
            print(f"[{node_id}] Updated term to {term}, following {leader_id}")

        reset_election_timeout()
        state = 'Follower'

        if entries:
            for entry in entries:
                log.append({'term': term, 'command': entry})
            print(f"[{node_id}] Appended {len(entries)} entries from leader {leader_id}")

        return jsonify({'term': current_term, 'success': True})

    @app.route('/submit_command', methods=['POST'])
    def submit_command():
        if state != 'Leader':
            return jsonify({'success': False, 'message': 'Not leader'})

        command = request.json['command']
        log.append({'term': current_term, 'command': command})
        print(f"[{node_id}] Leader appended: {command}")

        for peer in peers:
            send_append_entries(peer, [command])
        
        check_commit()
        return jsonify({'success': True, 'term': current_term})

    @app.route('/status', methods=['GET'])
    def status():
        return jsonify({
            'node_id': node_id,
            'state': state,
            'current_term': current_term,
            'log_length': len(log),
            'commit_index': commit_index,
            'voted_for': voted_for
        })

    @app.route('/health', methods=['GET'])
    def health():
        return jsonify({'status': 'healthy', 'node': node_id})

    if __name__ == '__main__':
        if len(sys.argv) < 7:
            print("Usage: python3 node.py --id <ID> --port <PORT> --peers <PEER1,PEER2>")
            sys.exit(1)

        node_id = sys.argv[2]
        port = int(sys.argv[4])
        peer_ids = sys.argv[6].split(',')

        peer_map = {
            'A': 'http://172.31.5.27:8000',
            'B': 'http://172.31.5.249:8001',
            'C': 'http://172.31.1.198:8002'
        }
        peers = [peer_map[p] for p in peer_ids]

        print(f"\n{'='*60}")
        print(f"Starting Node {node_id} on port {port}")
        print(f"Peers: {peers}")
        print(f"{'='*60}\n")

        # Запускаем таймер выборов
        threading.Thread(target=election_timer, daemon=True).start()

        # Запускаем Flask
        app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
