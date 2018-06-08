from urllib.request import urlopen, HTTPError
import time
import json


def access_url(url):
    try:
        print(url)
        response = urlopen(url)
        time.sleep(1)
        return response
    except HTTPError as e:
        print(e)
        return None
    except AttributeError as e:
        print(e)
        return None


# web 으로 부터 얻은 결과를 json 형태로 parsing 후 dictionary 형태로 반환
def get_json(url):
    response = access_url(url)
    return json.loads(response.read().decode())


