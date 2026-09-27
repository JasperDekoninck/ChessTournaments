import random
import json
from itertools import groupby
from flaskr.core import _annotate_dutch_pairing_state, _dutch_boards_for_pool, _pair_is_legal, _choose_colors, _colour_preference

def matchings(rows):
    if not rows:
        yield []
        return
    a=rows[0]
    for index,b in enumerate(rows[1:],1):
        if not _pair_is_legal(a,b): continue
        for rest in matchings(rows[1:index]+rows[index+1:]):
            yield [_choose_colors(a,b)]+rest

def quality(pairs):
    misses=strong=0
    for w,b in pairs:
        for p,c in ((w,'white'),(b,'black')):
            pref,strength=_colour_preference(p)
            if pref and pref!=c:
                misses+=1
                strong+=strength=='strong'
    return (misses,strong)

for seed in range(1000):
    rng=random.Random(seed)
    rows=[{'entry_id':i,'name':f'P{i}','score':0,'seed_rating':2500-i*50,'opponent_ids':set(),'colors':[],'color_rounds':[],'white_games':0,'black_games':0} for i in range(1,13)]
    history=[]
    for rnd in range(1,7):
        rows=_annotate_dutch_pairing_state(rows,7,rnd)
        boards=_dutch_boards_for_pool(rows,rnd)
        if not boards: break
        by_id={p['entry_id']:p for p in rows}
        if rnd>1:
            for score,g in groupby(sorted(rows,key=lambda p:p['score']),key=lambda p:p['score']):
                group=list(g)
                ids={p['entry_id'] for p in group}
                group_boards=[b for b in boards if b['white_entry_id'] in ids and b['black_entry_id'] in ids]
                if len(group)%2 or len(group)>8 or len(group_boards)*2!=len(group): continue
                pairs=[(by_id[b['white_entry_id']],by_id[b['black_entry_id']]) for b in group_boards]
                best=min(matchings(group),key=quality,default=None)
                if best is not None and quality(best)<quality(pairs):
                    result={'seed':seed,'round':rnd,'score':score,'history':history,'group':[dict(p,opponent_ids=sorted(p['opponent_ids'])) for p in group], 'actual':[(a['entry_id'],b['entry_id']) for a,b in pairs],'quality':quality(pairs),'better':[(a['entry_id'],b['entry_id']) for a,b in best],'better_quality':quality(best)}
                    print(json.dumps(result,indent=2))
                    open('/tmp/pairing-counterexample.json','w').write(json.dumps(result,indent=2))
                    raise SystemExit()
        rh=[]
        for board in boards:
            w,b=by_id[board['white_entry_id']],by_id[board['black_entry_id']]
            points=rng.choice([0,0.5,1])
            w['score']+=points
            b['score']+=1-points
            w['white_games']+=1
            b['black_games']+=1
            w['colors'].append('W')
            b['colors'].append('B')
            w['color_rounds'].append((rnd,'W'))
            b['color_rounds'].append((rnd,'B'))
            w['opponent_ids'].add(b['entry_id'])
            b['opponent_ids'].add(w['entry_id'])
            rh.append([w['entry_id'],b['entry_id'],points])
        history.append(rh)
print('No counterexample found')
