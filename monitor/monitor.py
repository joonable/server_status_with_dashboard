import json
import search.connector.status_scraper as sc

with open('./search/settings/params.json', 'r') as json_file:
    params = json.load(json_file)


class Monitor:
    # Main class 생성자
    def __init__(self):
        def set_job_list():
            scraper_names = [param["scraper_name"] for job, param in params.items()]
            scraper_names = set(scraper_names)
            job_list = {scraper_name: [] for scraper_name in scraper_names}

            for job_, value_ in params.items():
                job_list[value_['scraper_name']].append(job_)

            return job_list

        # list 내부 각각의 element 로 scraper 생성자 호출 (하는 역할에 따라 Scraper 구현)
        def set_scrapers(job_list, result):
            scrapers = list()
            scrapers.append(sc.ESIndexStatusScraper(job_list["ESIndex"], result))
            scrapers.append(sc.RankingFactorStatusScraper(job_list["RankingFactor"], result))
            scrapers.append(sc.ESClusterStatusScraper(job_list["ESCluster"], result))
            scrapers.append(sc.ZKStatusScraper(job_list["ZK"], result))
            return scrapers

        self.job_list = set_job_list()
        # self.job_list["ZK"] = ["44", "47"]

        all_job_list = []
        for job, value in self.job_list.items(): all_job_list += value
        # all_job_list = [job for job in params.keys() if job not in not_available_jobs]

        self._result = connector.result_data.Result(all_job_list=all_job_list)
        self._scrapers = set_scrapers(self.job_list, self._result)

        for scraper in self._scrapers:
            scraper.scrape_status()

    def get_data(self):
        return self._result.get_df()
