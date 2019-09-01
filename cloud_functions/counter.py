import os
import redis

redis_host = os.environ.get('REDISHOST', 'localhost')
redis_port = int(os.environ.get('REDISPORT', 6379))
redis_client = redis.StrictRedis(host=redis_host, port=redis_port)


def visit_count(request):
    val = request.args.get('val')
    if val == "crawl":
        num = request.args.get('num')
        value = redis_client.set(val, num)
        return 'Visit count: {}'.format(redis_client.get(val))
    else:
        value = redis_client.incr(val, 1)
        return 'Visit count: {}'.format(value)
