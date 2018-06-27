import redis
import uuid
import yaml

r = redis.StrictRedis(host="localhost", port=6379, db=0)

with open("rrq_data.yml", "r") as f:
    d = yaml.load(f.read())

with open("rrq_data.bin", "rb") as f:
    task_expr = f.read()

task_id = uuid.uuid4().hex

r.hset(d["task_expr"], task_id, task_expr)
r.hset(d["task_status"], task_id, "PENDING")
r.rpush(d["queue"], task_id)
