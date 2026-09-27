"""Read-only audit of application code; all database writes use temporary instances."""
import json
import tempfile
from pathlib import Path

from flaskr import create_app
from flaskr.core import (compute_standings, ensure_round_status_rows, fetch_pairings,
    generate_swiss_pairings, pairings_complete, replace_round_pairings)
from flaskr.db import get_db


def make_tournament(db, name, count, *, team=False, rounds=5, ratings=None):
    tid=db.execute('INSERT INTO tournament (name, slug, event_date, rounds_planned, is_team, excludes_rating) VALUES (?, ?, ?, ?, ?, 1)',(name,name,'2026-09-27',rounds,int(team))).lastrowid
    entries=[]
    for i in range(count):
        player_id=None if team else db.execute('INSERT INTO player (name, normalized_name) VALUES (?, ?)',(f'{name}-{i}',f'{name}-{i}')).lastrowid
        eid=db.execute('INSERT INTO tournament_entry (tournament_id, player_id, imported_name, seed_rating, member_status, is_active) VALUES (?, ?, ?, ?, ?, 1)',(tid,player_id,f'{name}-{i}',ratings[i] if ratings else 2400-i*100,'unknown')).lastrowid
        entries.append(eid)
    db.commit()
    ensure_round_status_rows(db,tid,rounds)
    return tid, entries


def save(db,tid,rnd,rows):
    replace_round_pairings(db,tid,rnd,[{'board_no':i,'white_entry_id':w,'black_entry_id':b,'result_code':r} for i,(w,b,r) in enumerate(rows,1)])


results={}
with tempfile.TemporaryDirectory() as temp:
    app=create_app({'INSTANCE_PATH':Path(temp),'TESTING':True,'SECRET_KEY':'audit-only','MAIL_ENABLED':False})
    client=app.test_client()
    with client.session_transaction() as session: session['is_admin']=True
    with app.app_context():
        db=get_db()
        tid,e=make_tournament(db,'team-colours',6,team=True)
        save(db,tid,1,[(e[0],e[1],'1-0'),(e[2],e[3],'1-0'),(e[4],e[5],'1-0')])
        save(db,tid,2,[(e[0],e[3],'1-0'),(e[2],e[5],'1-0'),(e[4],e[1],'1-0')])
        db.execute('UPDATE tournament_entry SET is_active=0 WHERE tournament_id=? AND id NOT IN (?,?)',(tid,e[0],e[2]))
        db.commit()
        boards=generate_swiss_pairings(db,tid,3)
        results['team_colour_restriction']={'expected':'one match between the two remaining teams','actual_boards':boards}
        assert not boards

        tid,e=make_tournament(db,'forfeit-bye',5,ratings=[1000,900,2000,800,1500])
        save(db,tid,1,[(e[0],e[1],'1F-0F'),(e[2],e[3],'1-0'),(e[4],None,'BYE')])
        db.execute('UPDATE tournament_entry SET is_active=0 WHERE id IN (?,?)',(e[1],e[3]))
        db.commit()
        boards=generate_swiss_pairings(db,tid,2)
        bye=next(b['white_entry_id'] for b in boards if b['black_entry_id'] is None)
        rows={p['entry_id']:p for p in compute_standings(db,tid,through_round=1)}
        results['forfeit_handling']={'bye_given_to_forfeit_winner':bye==e[0], 'forfeit_added_to_colour_history':rows[e[0]]['colors'],'forfeit_opponent_added_to_played_opponents':e[1] in rows[e[0]]['opponent_ids']}
        assert bye==e[0]

        tid,e=make_tournament(db,'round-save',4,rounds=3)
    form={'board_count':'2','white_1':e[0],'black_1':e[1],'result_1':'1-0','white_2':e[2],'black_2':e[3],'result_2':'1-0'}
    response=client.post('/admin/t/round-save/round/2/save',data=form)
    with app.app_context():
        results['skip_unplayed_round']={'http_status':response.status_code,'round_one_boards':len(fetch_pairings(get_db(),tid,1)),'round_two_boards':len(fetch_pairings(get_db(),tid,2))}
        assert len(fetch_pairings(get_db(),tid,2))==2
    response=client.post('/admin/t/round-save/round/99/save',data=form)
    with app.app_context():
        results['save_beyond_round_limit']={'planned':3,'saved_round':99,'boards':len(fetch_pairings(get_db(),tid,99))}
        assert len(fetch_pairings(get_db(),tid,99))==2
        db=get_db()
        db.execute('UPDATE tournament SET status="completed" WHERE id=?',(tid,))
        db.commit()
    form['result_1']='0-1'
    client.post('/admin/t/round-save/round/2/save',data=form)
    with app.app_context():
        db=get_db()
        results['finished_tournament_mutable']={'status':db.execute('SELECT status FROM tournament WHERE id=?',(tid,)).fetchone()[0],'changed_result':fetch_pairings(db,tid,2)[0]['result_code']}
        assert fetch_pairings(db,tid,2)[0]['result_code']=='0-1'
        tid,e=make_tournament(db,'invalid-result',4,rounds=3)
    form={'board_count':'2','white_1':e[0],'black_1':e[1],'result_1':'banana','white_2':e[2],'black_2':e[3],'result_2':'1-0'}
    client.post('/admin/t/invalid-result/round/1/save',data=form)
    with app.app_context():
        db=get_db()
        results['invalid_result_completes_round']={'stored_result':fetch_pairings(db,tid,1)[0]['result_code'],'round_complete':pairings_complete(db,tid,1)}
        assert pairings_complete(db,tid,1)
    client.post('/admin/t/invalid-result/round/2/generate')
    with app.app_context():
        results['invalid_result_completes_round']['next_round_boards']=len(fetch_pairings(get_db(),tid,2))
        assert len(fetch_pairings(get_db(),tid,2))==2

with tempfile.TemporaryDirectory() as temp:
    app=create_app({'INSTANCE_PATH':Path(temp),'TESTING':True,'SECRET_KEY':'audit-only','MAIL_ENABLED':False})
    client=app.test_client()
    with client.session_transaction() as session: session['is_admin']=True
    with app.app_context():
        tid,e=make_tournament(get_db(),'finished-results',2,rounds=1)
    form={'board_count':'1','white_1':e[0],'black_1':e[1],'result_1':'1-0'}
    client.post('/admin/t/finished-results/round/1/save',data=form)
    client.post('/admin/t/finished-results/complete')
    form['result_1']='0-1'
    client.post('/admin/t/finished-results/round/1/save',data=form)
    with app.app_context():
        db=get_db()
        cached={r['entry_id']:r['score'] for r in compute_standings(db,tid)}
        calculated={r['entry_id']:r['score'] for r in compute_standings(db,tid,prefer_stored=False)}
        results['finished_standings_stale']={'stored_scores':cached,'scores_from_results':calculated}
        assert cached!=calculated

print(json.dumps(results,indent=2))
Path('/tmp/chess-pairing-audit-results.json').write_text(json.dumps(results,indent=2)+'\n')
