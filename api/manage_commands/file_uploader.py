import re
from abc import ABC
from bs4 import BeautifulSoup
import requests
import warnings
from .base_uploader import BaseUploader, time_decorator

warnings.filterwarnings('ignore', message='Unverified HTTPS request')


class FileUploader(BaseUploader, ABC):
    def __init__(self):
        super().__init__()
        self.data = None

    @time_decorator
    def processing_web_object(self, path, changelog=False, count=0):
        try:
            resp = requests.get(path, verify=False, headers={'User-Agent': 'Mozilla/5.0'})
            if resp.status_code != 404:
                soup = BeautifulSoup(resp.text, 'lxml')
                if re.search("Release", path):
                    li = soup.get_text()
                    self.data = re.split(r"\n", li)
                if changelog:
                    vac_names = soup.findAll('a')
                    for i in vac_names:
                        s = i.get_text()
                        path_chang = path + "/" + s
                        if re.search(".dsc", s):
                            continue
                        if re.search("amd64.deb", s) or re.search("debian.tar.xz", s) or re.search("all.deb", s):
                            rl = requests.get(path_chang, verify=False, headers={'User-Agent': 'Mozilla/5.0'})
                            pc = path_chang.split("/")[-1]
                            with open(f"data/changelog_{pc}", 'wb') as f:
                                f.write(rl.content)
                            continue
                        if re.search(".tar", s):
                            continue
                        else:
                            self.processing_web_object(path_chang, changelog=True)
                else:
                    vac_names = soup.findAll('a')
                    for i in vac_names:
                        s = i.get_text()
                        if re.search("Packages", s):
                            url_rl = path + "/" + s
                            rl = requests.get(url_rl, verify=False, headers={'User-Agent': 'Mozilla/5.0'})
                            pc = url_rl.split("/")[-1]
                            st = url_rl.split("/")[-3]
                            with open(f"data/{st}_{pc}", 'wb') as f:
                                f.write(rl.content)
            else:
                if re.search("Release", path):
                    print("Wrong url!!!")
                    exit(1)
                return count + 1
        except Exception as e:
            print(e)
            self._error("getting data from web resource")
