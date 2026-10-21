"""Run against live APIs + Compose infrastructure. Creates isolated sample records."""
import concurrent.futures
import json
import subprocess
import time
import urllib.request
import urllib.error
import uuid

PRODUCT = 'http://127.0.0.1:5001/api/products'
ORDER = 'http://127.0.0.1:5002/api/orders'

def request(url, body=None):
    data = None if body is None else json.dumps(body).encode()
    with urllib.request.urlopen(urllib.request.Request(url, data=data, headers={'Content-Type':'application/json'}), timeout=10) as response:
        return json.load(response)

def eventually(fn, expected, seconds=90):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            actual = fn()
            if actual == expected:
                return
        except (urllib.error.URLError, ConnectionError):
            pass
        time.sleep(.5)
    raise AssertionError(f'Expected {expected}, last result: {locals().get("actual")}')

def compose(*args, **kwargs):
    return subprocess.run(['docker','compose',*args], check=True, **kwargs)

def order(product, quantity):
    return request(ORDER, {'productId':product['id'], 'quantity':quantity})

def main():
    for port in [5001,5002]:
        url=f'http://127.0.0.1:{port}/swagger/v1/swagger.json'
        eventually(lambda: bool(request(url)['paths']), True)
    p=request(PRODUCT, {'name':'Smoke product','price':25,'stock':10})
    o=order(p,3)
    eventually(lambda: request(ORDER+'/'+o['id'])['status'], 'Confirmed')
    eventually(lambda: request(PRODUCT+'/'+p['id'])['stock'], 7)
    rejected=order(p,20)
    eventually(lambda: request(ORDER+'/'+rejected['id'])['status'], 'Rejected')
    assert request(PRODUCT+'/'+p['id'])['stock'] == 7
    missing=order({'id':str(uuid.uuid4())},1)
    eventually(lambda: request(ORDER+'/'+missing['id'])['status'], 'Rejected')
    try:
        order(p,0)
        raise AssertionError('Invalid order accepted')
    except urllib.error.HTTPError as error:
        assert error.code == 400
    print('PASS: Swagger, success, insufficient stock, missing product, validation', flush=True)

    # Replay the actual original event twice, including its original EventId.
    query=f'''SELECT "Payload" FROM "Outbox" WHERE "Key" = '{o['id']}';'''
    payload=subprocess.check_output(['docker','compose','exec','-T','postgres','psql','-U','app','-d','orders','-Atc',query],text=True).strip()
    compose('exec','-T','kafka','/opt/kafka/bin/kafka-console-producer.sh','--bootstrap-server','localhost:29092',
            '--topic','orders.created.v1','--property','parse.key=true','--property','key.separator=|',
            input=f"{o['id']}|{payload}\n{o['id']}|{payload}\n",text=True,stdout=subprocess.DEVNULL)
    ends=subprocess.check_output(['docker','compose','exec','-T','kafka','/opt/kafka/bin/kafka-get-offsets.sh',
        '--bootstrap-server','localhost:29092','--topic','orders.created.v1'],text=True)
    targets={int(line.split(':')[1]):int(line.split(':')[2]) for line in ends.splitlines() if line.startswith('orders.created.v1:')}
    def replay_committed():
        output=subprocess.check_output(['docker','compose','exec','-T','kafka','/opt/kafka/bin/kafka-consumer-groups.sh',
            '--bootstrap-server','localhost:29092','--group','product-service-v1','--describe'],text=True)
        offsets={}
        for line in output.splitlines():
            fields=line.split()
            if len(fields)>3 and fields[0]=='product-service-v1' and fields[1]=='orders.created.v1' and fields[3].isdigit():
                offsets[int(fields[2])]=int(fields[3])
        return all(offsets.get(partition,-1)>=end for partition,end in targets.items())
    eventually(replay_committed,True)
    assert request(PRODUCT+'/'+p['id'])['stock']==7, 'Duplicate event changed stock'
    print('PASS: duplicate delivery does not deduct stock twice', flush=True)

    concurrent_product=request(PRODUCT, {'name':'Concurrent stock','price':5,'stock':5})
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
        orders=list(pool.map(lambda _: order(concurrent_product,1), range(10)))
    for item in orders:
        eventually(lambda: request(ORDER+'/'+item['id'])['status'] != 'Pending',True)
    statuses=[request(ORDER+'/'+item['id'])['status'] for item in orders]
    assert statuses.count('Confirmed')==5 and statuses.count('Rejected')==5, statuses
    assert request(PRODUCT+'/'+concurrent_product['id'])['stock']==0
    print('PASS: concurrent orders cannot oversell', flush=True)

    compose('stop','kafka',stdout=subprocess.DEVNULL)
    try:
        pending=order(p,1)
        assert pending['status']=='Pending'
        time.sleep(2)
        assert request(ORDER+'/'+pending['id'])['status']=='Pending'
    finally:
        compose('start','kafka',stdout=subprocess.DEVNULL)
    eventually(lambda: request(ORDER+'/'+pending['id'])['status'],'Confirmed',120)
    eventually(lambda: request(PRODUCT+'/'+p['id'])['stock'],6)
    print('PASS: Kafka outage preserves order; outbox delivers after restart', flush=True)

if __name__=='__main__':
    main()
