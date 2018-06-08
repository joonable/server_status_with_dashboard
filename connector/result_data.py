import sys
import datetime
import pandas as pd

sys.path.insert(0, '../settings/')
sys.path.insert(0, '../monitor/')
sys.path.insert(0, '../connector/')


# scraping을 통해 얻은 결과를 저장하고 db에 적재하는 class
class Result(object):

    def __init__(self, all_job_list):
        self.__df = pd.DataFrame(index=[all_job_list], columns=["value"])

    # job 번호를 index로 하여 value 설정
    def set_value(self, job, value):
        self.__df.loc[job, "value"] = value

    # df를 db로 보내기 위한 최종형태로 변환
    def prepare_data_to_send(self):
        input_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        self.__df = self.__df.reset_index(drop=False)
        self.__df.columns = ["CODE_ID", "MNTPNG_RESULT"]
        self.__df['INPUT_TIME'] = input_time

    def send_result_to_db(self):
        print(self.__df)
        pass

    def get_df(self):
        return self.__df

    def __call__(self):
        print(self.__df)
