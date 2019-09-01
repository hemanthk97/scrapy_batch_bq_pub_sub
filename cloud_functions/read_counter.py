import os
import redis

redis_host = os.environ.get('REDISHOST', 'localhost')
redis_port = int(os.environ.get('REDISPORT', 6379))
redis_client = redis.StrictRedis(host=redis_host, port=redis_port)


def visit_count(request):
    val = request.args.get('val')
    if val == "crawl":
        return 'Visit count: {}'.format(redis_client.get(val))
    else:
        set = request.args.get('set')
        if set:
           	redis_client.set(val,0)
        return 'Visit count: {}'.format(redis_client.get(val))
