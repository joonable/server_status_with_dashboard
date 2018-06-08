import abc
import sys
import pymysql
from kazoo.client import KazooClient

from search.utils.time_util import *
from search.utils.url_util import *

sys.path.insert(0, '../settings/')
sys.path.insert(0, '../monitor/')
sys.path.insert(0, '../connector/')

with open('./search/settings/params.json', 'r', encoding='utf-8') as parameter_file:
    params = json.load(parameter_file)

with open('./search/settings/config.json', 'r') as config_file:
    config = json.load(config_file)


# StatusScraper = base class
class StatusScraper(abc.ABC):

    # StatusScraper 생성자로서 derived class 생성자 호출 전 호출됨
    def __init__(self, scraper_name):
        abc.ABC.__init__(self)
        self._scraper_name = scraper_name
        self._job_list = None
        self._config = None
        self._result = None

    # StatusScraper 생성자로서 derived class 생성자 호출 후 호출됨
    def _post_init(self, result):
        # derived class마다 사용되는 config이 다르므로 drived class 에 필요한 config을 불러옴
        self._config = config[self._scraper_name]
        self._result = result

    # job_list 가 유효한지 확인하고 유효할 경우 self.job_list 에 assignment
    def _assert_job_list(self, job_list):
        self._job_list = config[self._scraper_name]['job_list']
        for job in job_list:
            assert job in self._job_list, print(job_list, self._job_list)
        self._job_list = job_list

    # server 혹은 web에 접근하여 status를 얻음
    # derived class 함수 호출 전 호출되며 현재 상태를 보여줌
    @abc.abstractmethod
    def scrape_status(self):
        print("\ngetting " + self._scraper_name + " status")


# ESIndexStatusScraper = genie server 의 alias 를 통해 index 의 status 를 파악하는 class
class ESIndexStatusScraper(StatusScraper):
    def __init__(self, job_list, result):
        StatusScraper.__init__(self, "ESIndex")
        self._assert_job_list(job_list)
        self._post_init(result)

    def scrape_status(self):
        StatusScraper.scrape_status(self)
        for job in self._job_list:
            param = params[job]
            host = self._config[param['cluster']]

            if "alias" in param.keys():
                url = host + "/alias_"+ param["alias"] +"/_search"
            else:
                yesterday = get_yesterday()
                url = host + "/idx_" + param["index_name"] + yesterday.strftime("_%m%d") + "/_search?q=chanl_nm:" \
                      + param["chanl_nm"] + "&size=1"
            json_file = get_json(url)

            value = json_file["hits"]["total"]
            self._result.set_value(job, value)


# ESClusterStatusScraper = pinpoint 를 통해 es cluster 의 status 와 response summary 를 파악하는 class
class ESClusterStatusScraper(StatusScraper):
    def __init__(self, job_list, result):
        StatusScraper.__init__(self, "ESCluster")
        self._assert_job_list(job_list)
        self._post_init(result)

    def scrape_status(self):
        StatusScraper.scrape_status(self)
        current_cluster = None
        result = {}

        def get_health(host):
            url = host + "/_cluster/health/"
            json_file = get_json(url)
            status = json_file["status"]
            return status

        def access_pinpoint_page(host, _cluster):
            yesterday = get_yesterday()
            url = host + "/#/main/" + _cluster + "-QPLANNER@TOMCAT/1439m/" \
                  + yesterday.strftime("%Y-%m-%d") + "-23-59-00"

            access_url(url)
            time.sleep(3)

        def get_api_response(host, _cluster):
            access_pinpoint_page(host=host, _cluster=_cluster)

            yesterday = get_yesterday()
            from_time_stamp = get_time_stamp(yesterday, "0000") + "000"
            to_time_stamp = get_time_stamp(yesterday, "2359") + "000"

            url = host + "/getServerMapData.pinpoint?applicationName=" + _cluster + \
                  "-QPLANNER&from=" + from_time_stamp + "&to=" + to_time_stamp + "&serviceTypeName=TOMCAT"

            json_file = get_json(url)
            node_data_array = json_file['applicationMapData']['nodeDataArray']
            for element in node_data_array:
                if element["key"] == _cluster + "-QPLANNER^TOMCAT":
                    temp = element["histogram"]
                    return temp

        for job in self._job_list:
            param = params[job]

            # 접근해야할 cluster 가 바뀔 때만 접근
            if param["cluster"] != current_cluster:
                current_cluster = param["cluster"]
                api_host = self._config['api_host']
                cluster = current_cluster.upper()
                result = get_api_response(host=api_host, _cluster=cluster)
                result["status"] = get_health(host=self._config[current_cluster])

            value = result[param["value"]]
            self._result.set_value(job=job, value=value)


# RankingFactorStatusScraper = mysql query 를 통해 ranking factor 의 status 를 파악하는 class
class RankingFactorStatusScraper(StatusScraper):
    def __init__(self, job_list, result):
        StatusScraper.__init__(self, "RankingFactor")
        self._assert_job_list(job_list)
        self._post_init(result)

    def scrape_status(self):
        StatusScraper.scrape_status(self)
        for job in self._job_list:
            param = params[job]
            table_name = param["table_name"]
            print(table_name)
            # Connect to the database
            try:
                connection = pymysql.connect(host=self._config["db_host"],
                                             user=self._config["db_user"],
                                             password=self._config["db_pw"],
                                             db="gsshop",
                                             cursorclass=pymysql.cursors.DictCursor)

                with connection.cursor() as cursor:
                    sql = "SELECT COUNT(*) FROM " + table_name
                    cursor.execute(sql)
                    result = cursor.fetchone()
                    for key, value in result.items():
                        self._result.set_value(job=job, value=value)

            except pymysql.Error as e:
                print(e)


# ZKStatusScraper = zk 서버에 접속해 znode의 수와 status를 파악하는 class
class ZKStatusScraper(StatusScraper):
    def __init__(self, job_list, result):
        StatusScraper.__init__(self, "ZK")
        self._assert_job_list(job_list)
        self._post_init(result)

    def scrape_status(self):
        StatusScraper.scrape_status(self)

        def get_num_znodes(host):
            temp = {}
            zk = KazooClient(hosts=host, read_only=True)
            zk.start()
            temp["gmas"] = len(zk.get_children('/home/gmas'))
            temp["amas"] = len(zk.get_children('/home/amas'))
            zk.stop()
            zk.close()
            return temp

        result = dict()
        result["staging"] = get_num_znodes(self._config["staging_host"])
        result["production"]= get_num_znodes(self._config["production_host"])      #TODO production db 열어야함

        for job in self._job_list:
            param = params[job]
            server_name = param["server_name"]
            
            if param["value"] == "status":
                if self._config["num_" + server_name] == result["production"][server_name]:
                    value = "green"
                else:
                    value = "red"
            else:
                value = result[param["value"]][server_name]

            self._result.set_value(job=job, value=value)
