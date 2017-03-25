import requests
import threading
import csv
import time
import random

lock=threading.Lock()

class MyThread(threading.Thread):

    #header
    header={
            "Host": "m.weibo.cn",
            "User - Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0",
            "Accept": "application/json, text/plain, */*",
            "Accept - Language": "en-US,en;q=0.5",
            "Accept - Encoding": "gzip, deflate",
            "Referer": "",
            "X - Requested - With": "XMLHttpRequest",
            "Connection": "keep-alive"
        }

    def __init__(self,taskname,direction,depth,people,width,start_list,interval):
        threading.Thread.__init__(self)
        self.taskname=taskname
        self.direction=direction
        self.max_depth=depth
        self.max_people=people
        self.max_width=width
        self.start_list=start_list
        self.peoplenum=0
        self.depth=1
        self.interval=interval
        #noparse_flag
        self.__nopause=threading.Event()
        self.__nopause.set()
        #nostop_flag
        self.__running=threading.Event()
        self.__running.set()

    def pause(self):
        self.__nopause.clear()

    def resume(self):
        self.__nopause.set()

    def stop(self):
        self.__running.clear()

    def run(self):
        result=self.start_list
        loop=[]
        if self.direction=="fans":
            while result != None and self.depth <= self.max_depth and self.peoplenum <= self.max_people and self.__running.is_set():
                for item in result:
                    user = item["user"]
                    info = []
                    info.append(user["id"])
                    info.append(user["screen_name"])
                    info.append(user["verified"])
                    info.append(user["description"])
                    info.append(user["gender"])
                    info.append(user["followers_count"])
                    info.append(user["follow_count"])
                    lock.acquire()
                    try:
                        with open(self.taskname+".csv", 'a', errors='ignore', newline='') as f:
                            ff = csv.writer(f)
                            ff.writerow(info)
                            f.close()
                    finally:
                        self.peoplenum += 1
                        lock.release()
                        loop.append(info[0])
                self.depth+=1

                # parse
                self.__nopause.wait()
                if not self.__nopause.is_set():
                    break

                result = []
                for item in loop:

                    #stop_guess
                    if not self.__running.is_set():
                        break

                    url = "http://m.weibo.cn/container/getSecond?containerid=100505" + str(item) + "_-_FANS"
                    self.header["Referer"] = url
                    try:
                        content = requests.get(url, self.header).json()["cards"]
                        time.sleep(self.interval)
                        if len(content) > self.max_width:
                            content = content[0:self.max_width]
                        for item in content:
                            result.append(item)
                    except:
                        continue

        elif self.direction=="followers":
            while result != None and self.depth <= self.max_depth and self.peoplenum <= self.max_people and self.__running.is_set():
                for item in result:
                    user = item["user"]
                    info = []
                    info.append(user["id"])
                    info.append(user["screen_name"])
                    info.append(user["verified"])
                    info.append(user["description"])
                    info.append(user["gender"])
                    info.append(user["followers_count"])
                    info.append(user["follow_count"])
                    lock.acquire()
                    try:
                        with open(self.taskname + ".csv", 'a', errors='ignore', newline='') as f:
                            ff = csv.writer(f)
                            ff.writerow(info)
                            f.close()
                    finally:
                        self.peoplenum += 1
                        lock.release()
                        loop.append(info[0])
                self.depth += 1

                self.__nopause.wait()
                if not self.__nopause.is_set():
                    break

                result = []
                for item in loop:

                    if not self.__running.is_set():
                        break

                    url = "http://m.weibo.cn/container/getSecond?containerid=100505" + str(item) + "_-_FOLLOWERS"
                    self.header["Referer"] = url
                    try:
                        content = requests.get(url, self.header).json()["cards"]
                        time.sleep(self.interval)
                        if len(content) > self.max_width:
                            content = content[0:self.max_width]
                        for item in content:
                            result.append(item)
                    except:
                        continue

        else:
            while result != None and self.depth <= self.max_depth and self.peoplenum <= self.max_people and self.__running.is_set():
                for item in result:
                    user = item["mblog"]["user"]
                    info = []
                    info.append(user["id"])
                    info.append(user["screen_name"])
                    info.append(user["verified"])
                    info.append(user["description"])
                    info.append(user["gender"])
                    info.append(user["followers_count"])
                    info.append(user["follow_count"])
                    lock.acquire()
                    try:
                        with open(self.taskname + ".csv", 'a', errors='ignore', newline='') as f:
                            ff = csv.writer(f)
                            ff.writerow(info)
                            f.close()
                    finally:
                        self.peoplenum += 1
                        lock.release()
                        loop.append(info[0])
                self.depth += 1

                self.__nopause.wait()
                if not self.__nopause.is_set():
                    break

                result = []
                for item in loop:

                    if not self.__running.is_set():
                        break

                    url = "http://m.weibo.cn/container/getSecond?containerid=100505" + str(item) + "_ - _WEIBO_SECOND_PROFILE_LIKE_WEIBO"
                    self.header["Referer"] = url
                    try:
                        content = requests.get(url, self.header).json()["cards"]
                        time.sleep(self.interval)
                        if len(content) > self.max_width:
                            content = content[0:self.max_width]
                        for item in content:
                            result.append(item)
                    except:
                        continue


class SpiderWb():

    def __init__(self,start_user,task_name,threads,direction="fans",spider_depth=1,spider_time=30,spider_people=100,spider_width=10,spider_interval=2,proxy={}):
        self.__start_user=start_user
        self.__task_name=task_name
        self.__threads=threads
        self.__direction=direction
        self.__spider_depth=spider_depth
        self.__spider_time=spider_time
        self.__spider_people=spider_people
        self.__spider_width=spider_width
        self.__proxy=proxy
        self.spider_interval=spider_interval
        self.fans_thread=[]
        self.follow_thread=[]
        self.like_thread=[]
        self.__false=False

    def start(self):

        #direction : "fans"
        if self.__direction=="fans":
            #start_url   start_header
            fans_url="http://m.weibo.cn/container/getSecond?containerid=100505"+str(self.__start_user)+"_-_FANS"
            fans_header={
                "Host": "m.weibo.cn",
                "User - Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0",
                "Accept": "application/json, text/plain, */*",
                "Accept - Language": "en-US,en;q=0.5",
                "Accept - Encoding": "gzip, deflate",
                "Referer": fans_url,
                "X - Requested - With": "XMLHttpRequest",
                "DNT": "1",
                "Connection": "keep-alive"
            }
            fans_response=requests.get(fans_url,fans_header,proxies=self.__proxy,timeout=random.choice(range(100,120)))
            fans_json=fans_response.json()
            try:
                fans_content=fans_json["cards"]
                count=int(len(fans_content)/self.__threads)
            except:
                print("数据加载失败!")
                self.__false=True
                return False

            #write some info
            with open(self.__task_name + ".csv", 'a', errors='ignore', newline='') as f:
                ff = csv.writer(f)
                ff.writerow(["id","screen_name","verified","description","gender","followers_count","follow_count"])
                f.close()

            #mutiple thread

            for i in range(self.__threads):
                self.fans_thread.append(MyThread(taskname=self.__task_name,
                                            direction=self.__direction,
                                            depth=self.__spider_depth,
                                            people=self.__spider_people,
                                            width=self.__spider_width,
                                            start_list=fans_content[i*count:(i+1)*count],
                                            interval=self.spider_interval))
            for i in self.fans_thread:
                i.start()


        #direction : "followers"
        elif self.__direction=="followers":
            follow_url = "http://m.weibo.cn/container/getSecond?containerid=100505" + str(self.__start_user) + "_-_FOLLOWERS"
            follow_header={
                "Host": "m.weibo.cn",
                "User - Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0",
                "Accept": "application/json, text/plain, */*",
                "Accept - Language": "en-US,en;q=0.5",
                "Accept - Encoding": "gzip, deflate",
                "Referer": follow_url,
                "X - Requested - With": "XMLHttpRequest",
                "Connection": "keep-alive"
            }
            follow_response=requests.get(follow_url,follow_header,proxies=self.__proxy,timeout=random.choice(range(100,120)))
            follow_json=follow_response.json()
            try:
                follow_content=follow_json["cards"]
                count=int(len(follow_content)/self.__threads)
            except:
                print("数据加载失败!")
                self.__false=True
                return False

            #write some info
            with open(self.taskname + ".csv", 'a', errors='ignore', newline='') as f:
                ff = csv.writer(f)
                ff.writerow(["id","screen_name","verified","description","gender","followers_count","follow_count"])
                f.close()

            #mutiple_thread
            for i in range(self.__threads):
                self.follow_thread.append(MyThread(taskname=self.__task_name,
                                              direction=self.__direction,
                                              depth=self.__spider_depth,
                                              people=self.__spider_people,
                                              width=self.__spider_width,
                                              start_list=follow_content[i*count:(i+1)*count],
                                              interval=self.spider_interval))
            for i in self.follow_thread:
                i.start()


        #direction : "likes"
        else:
            likes_url="http://m.weibo.cn/container/getSecond?containerid=100505"+ str(self.__start_user) +"_-_WEIBO_SECOND_PROFILE_LIKE_WEIBO"
            likes_header = {
                "Host": "m.weibo.cn",
                "User - Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept - Language": "en-US,en;q=0.5",
                "Accept - Encoding": "gzip, deflate",
                "Upgrade-Insecure-Requests":"1",
                "Connection": "keep-alive",
                "Cache - Control": "max-age=0"
            }
            likes_response=requests.get(likes_url,likes_header,proxies=self.__proxy,timeout=random.choice(range(100,120)))
            likes_json=likes_response.json()
            try:
                likes_count=likes_json["count"]
                likes_content=likes_json["cards"]
                count = int(likes_count/ self.__threads)
            except:
                print("数据加载失败!")
                self.__false=True
                return False

            # write some info
            with open(self.__task_name + ".csv", 'a', errors='ignore', newline='') as f:
                ff = csv.writer(f)
                ff.writerow(
                    ["id", "screen_name", "verified", "description", "gender", "followers_count", "follow_count"])
                f.close()

            # mutiple_thread
            for i in range(self.__threads):
                self.like_thread.append(MyThread(taskname=self.__task_name,
                                                   direction=self.__direction,
                                                   depth=self.__spider_depth,
                                                   people=self.__spider_people,
                                                   width=self.__spider_width,
                                                   start_list=likes_content[i * count:(i + 1) * count],
                                                   interval=self.spider_interval))
            for i in self.like_thread:
                i.start()


    def parse(self):
        if self.__direction=="fans":
            for i in self.fans_thread:
                i.pause()
        elif self.__direction=="followers":
            for i in self.follow_thread:
                i.pause()
        else:
            for i in self.like_thread:
                i.pause()


    def resume(self):
        if self.__direction=="fans":
            for i in self.fans_thread:
                i.resume()
        elif self.__direction=="followers":
            for i in self.follow_thread:
                i.resume()
        else:
            for i in self.like_thread:
                i.resume()


    def stop(self):
        if self.__direction=="fans":
            for i in self.fans_thread:
                i.stop()
            for i in self.fans_thread:
                i.join()

        elif self.__direction=="followers":
            for i in self.follow_thread:
                i.stop()
            for i in self.follow_thread:
                i.join()

        else:
            for i in self.like_thread:
                i.stop()
            for i in self.like_thread:
                i.join()


    def time_over(self):
        if self.__false==False:
            time.sleep(self.__spider_time)
            self.stop()



class GetInfo():
    def __init__(self,filename,choice=[0,1,2,3,4,5,6]):
        self.__filename=str(filename)
        self.__choice=choice

    def getAll(self):
        r=csv.reader(open(self.__filename,'r'))
        final=[]
        next(r)
        for row in r:
            res=[]
            for index,item in enumerate(row):
                if index in self.__choice:
                    res.append(item)
            final.append(res)
        print(final)




if __name__=="__main__":
    proxy={

    }
    sw=SpiderWb(start_user="5822209475",task_name="like",threads=4,direction="likes",spider_depth=5,proxy=proxy)
    sw.start()
    sw.time_over()
    #GetInfo("fans.csv").getAll()