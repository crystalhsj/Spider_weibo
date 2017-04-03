import requests
import threading
import csv
import time
import random
import math
import re

lock=threading.Lock()

#this is a dictionary,which is used to filter unique ids.
dic={}

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
        # loop[] :store ids of next depth.
        loop=[]
        if self.direction=="fans":
            #stop if fit one of these conditions.
            while result != None and self.depth <= self.max_depth and self.peoplenum <= self.max_people and self.__running.is_set():
                for item in result:
                    #choose ids having not spider
                    if item["user"]["id"] not in dic:
                        # add id to dic
                        dic[item["user"]["id"]] = None
                        # stop_guess
                        if not self.__running.is_set():
                            break
                        #choose paramters to spider.
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
                            #wonder whether peoplenum is bigger than we need.
                            if self.peoplenum<self.max_people:
                                with open(self.taskname+".csv", 'a', errors='ignore', newline='') as f:
                                    ff = csv.writer(f)
                                    ff.writerow(info)
                                    f.close()
                        finally:
                            self.peoplenum += 1
                            lock.release()
                            #if peoplenum is bigger,then stop.
                            if self.peoplenum>=self.max_people:
                                loop=[]
                                break
                            loop.append(info[0])

                #increase depth
                self.depth+=1
                if self.depth>self.max_depth:
                    break

                if self.__running.is_set():
                    # parse
                    self.__nopause.wait()
                    if not self.__nopause.is_set():
                        break

                result = []
                for item in loop:
                    #stop_guess
                    if not self.__running.is_set():
                        break
                    try:
                        count=0
                        page=1

                        #just choose number of people we need
                        while count<self.max_width:
                            url = "http://m.weibo.cn/container/getSecond?containerid=100505" + str(item) + "_-_FANS&page="+str(page)
                            self.header["Referer"] = url
                            try:
                                content = requests.get(url, self.header).json()["cards"]
                                time.sleep(self.interval)
                                for j in content:
                                    if count<self.max_width:
                                        result.append(j)
                                        count=count+1
                                    else:
                                        break
                            except:
                                break
                            finally:
                                page=page+1
                    except:
                        continue

        elif self.direction=="followers":
            # stop if fit one of these conditions.
            while result != None and self.depth <= self.max_depth and self.peoplenum <= self.max_people and self.__running.is_set():
                for item in result:
                    # choose ids having not spider
                    if item["user"]["id"] not in dic:
                        # add id to dic
                        dic[item["user"]["id"]] = None
                        # stop_guess
                        if not self.__running.is_set():
                            break
                        # choose paramters to spider.
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
                            # wonder whether peoplenum is bigger than we need.
                            if self.peoplenum < self.max_people:
                                with open(self.taskname + ".csv", 'a', errors='ignore', newline='') as f:
                                    ff = csv.writer(f)
                                    ff.writerow(info)
                                    f.close()
                        finally:
                            self.peoplenum += 1
                            lock.release()
                            # if peoplenum is bigger,then stop.
                            if self.peoplenum >= self.max_people:
                                loop = []
                                break
                            loop.append(info[0])

                # increase depth
                self.depth += 1
                if self.depth > self.max_depth:
                    break

                if self.__running.is_set():
                    # parse
                    self.__nopause.wait()
                    if not self.__nopause.is_set():
                        break

                result = []
                for item in loop:
                    # stop_guess
                    if not self.__running.is_set():
                        break
                    try:
                        count = 0
                        page = 1

                        # just choose number of people we need
                        while count < self.max_width:
                            url = "http://m.weibo.cn/container/getSecond?containerid=100505" + str(
                                item) + "_-_FOLLOWERS&page=" + str(page)
                            self.header["Referer"] = url
                            try:
                                content = requests.get(url, self.header).json()["cards"]
                                time.sleep(self.interval)
                                for j in content:
                                    if count < self.max_width:
                                        result.append(j)
                                        count = count + 1
                                    else:
                                        break
                            except:
                                break
                            finally:
                                page = page + 1
                    except:
                        continue

        else:
            # stop if fit one of these conditions.
            while result != None and self.depth <= self.max_depth and self.peoplenum <= self.max_people and self.__running.is_set():
                for item in result:
                    # stop_guess
                    if not self.__running.is_set():
                        break
                    # choose paramters to spider.
                    try:
                        user = item["mblog"]["user"]
                        info = []
                        info.append(user["id"])
                        info.append(user["screen_name"])
                        info.append(user["verified"])
                        info.append(user["description"])
                        info.append(user["gender"])
                        info.append(user["followers_count"])
                        info.append(user["follow_count"])
                        info.append(item["from_me"])
                        text=item["mblog"]["text"]
                        text=re.sub(re.compile(r"<.*>"),"",text)
                        info.append(text)
                    except:
                        continue
                    lock.acquire()
                    try:
                        # wonder whether peoplenum is bigger than we need.
                        if self.peoplenum < self.max_people:
                            with open(self.taskname + ".csv", 'a', errors='ignore', newline='') as f:
                                ff = csv.writer(f)
                                ff.writerow(info)
                                f.close()
                    finally:
                        self.peoplenum += 1
                        lock.release()
                        # if peoplenum is bigger,then stop.
                        if self.peoplenum >= self.max_people:
                            loop = []
                            break
                        if user["id"] not in dic:
                            loop.append(info[0])
                            # add id to dic
                            dic[info[0]] = None

                # increase depth
                self.depth += 1
                if self.depth > self.max_depth:
                    break

                if self.__running.is_set():
                    self.__nopause.wait()
                    if not self.__nopause.is_set():
                        break

                result = []
                for item in loop:
                    if not self.__running.is_set():
                        break
                    try:
                        count = 0
                        page = 1
                        # just choose number of people we need
                        while count < self.max_width:
                            url = "http://m.weibo.cn/container/getSecond?containerid=100505" + str(item) + "_-_WEIBO_SECOND_PROFILE_LIKE_WEIBO&page=" + str(page)
                            self.header["Referer"] = url
                            try:
                                content = requests.get(url, self.header).json()["cards"]
                                time.sleep(self.interval)
                                for j in content:
                                    #add from recourse
                                    j["from_me"]=item
                                    if count < self.max_width:
                                        result.append(j)
                                        count = count + 1
                                    else:
                                        break
                            except:
                                break
                            finally:
                                page = page + 1
                    except:
                        continue


class SpiderWb():

    def __init__(self,start_user,task_name,threads,direction="fans",spider_depth=1,spider_time=200,spider_people=100,spider_width=9,spider_interval=2,proxy={}):
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
            fans_content=[]
            count=0
            page=1
            while count<self.__spider_width:
                #start_url   start_header
                fans_url="http://m.weibo.cn/container/getSecond?containerid=100505"+str(self.__start_user)+"_-_FANS&page="+str(page)
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
                try:
                    fans_response=requests.get(fans_url,fans_header,proxies=self.__proxy,timeout=random.choice(range(100,120)))
                    fans_json=fans_response.json()
                    for i in fans_json["cards"]:
                        if count<self.__spider_width:
                            fans_content.append(i)
                            count=count+1
                        else:
                            break
                except:
                    if fans_content==None:
                        self.__false=True
                        print("数据加载失败!")
                        return False
                    else:
                        break
                finally:
                    page=page+1

            count = math.ceil(self.__spider_width/ self.__threads)
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
                                            people=int(self.__spider_people/self.__threads),
                                            width=self.__spider_width,
                                            start_list=fans_content[i*count:(i+1)*count],
                                            interval=self.spider_interval))
            for i in self.fans_thread:
                i.start()


        #direction : "followers"
        elif self.__direction=="followers":
            follow_content = []
            count = 0
            page = 1
            while count < self.__spider_width:
                follow_url = "http://m.weibo.cn/container/getSecond?containerid=100505" + str(self.__start_user) + "_-_FOLLOWERS&page="+str(page)
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
                try:
                    follow_response=requests.get(follow_url,follow_header,proxies=self.__proxy,timeout=random.choice(range(100,120)))
                    follow_json=follow_response.json()
                    for i in follow_json["cards"]:
                        if count < self.__spider_width:
                            follow_content.append(i)
                            count = count + 1
                        else:
                            break
                except:
                    if follow_content == None:
                        self.__false = True
                        print("数据加载失败!")
                        return False
                    else:
                        break
                finally:
                    page = page + 1

            count = math.ceil(self.__spider_width / self.__threads)
            #write some info
            with open(self.__task_name + ".csv", 'a', errors='ignore', newline='') as f:
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
            likes_content = []
            count = 0
            page = 1
            while count < self.__spider_width:
                likes_url="http://m.weibo.cn/container/getSecond?containerid=100505"+ str(self.__start_user) +"_-_WEIBO_SECOND_PROFILE_LIKE_WEIBO&page="+str(page)
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
                try:
                    likes_response=requests.get(likes_url,likes_header,proxies=self.__proxy,timeout=random.choice(range(100,120)))
                    likes_json=likes_response.json()
                    for i in likes_json["cards"]:
                        i["from_me"]=self.__start_user
                        if count < self.__spider_width:
                            likes_content.append(i)
                            count = count + 1
                        else:
                            break
                except:
                    if likes_content == None:
                        self.__false = True
                        print("数据加载失败!")
                        return False
                    else:
                        break
                finally:
                    page = page + 1

            count = math.ceil(self.__spider_width / self.__threads)
            # write some info
            with open(self.__task_name + ".csv", 'a', errors='ignore', newline='') as f:
                ff = csv.writer(f)
                ff.writerow(
                    ["id", "screen_name", "verified", "description", "gender", "followers_count", "follow_count","from_me","text"])
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
        elif self.__direction=="followers":
            for i in self.follow_thread:
                i.stop()
        else:
            for i in self.like_thread:
                i.stop()


    def time_over(self):
        if self.__false==False:
            if self.__direction=="fans":
                thread = Time_over(self.__spider_time,self.fans_thread)
                thread.start()
                while True in map(lambda x:x.isAlive(),self.fans_thread):
                    pass
                if thread.isAlive():
                    thread.stop()
            elif self.__direction=="followers":
                thread = Time_over(self.__spider_time,self.follow_thread)
                thread.start()
                while True in map(lambda x:x.isAlive(),self.follow_thread):
                    pass
                if thread.isAlive():
                    thread.stop()
            else:
                thread = Time_over(self.__spider_time, self.like_thread)
                thread.start()
                while True in map(lambda x:x.isAlive(),self.like_thread):
                    pass
                if thread.isAlive():
                    thread.stop()
        else:
            print("数据加载失败!")



class Time_over(threading.Thread):
    def __init__(self,time,threads):
        threading.Thread.__init__(self)
        self.__time=time
        self.running=True
        self.__threads=threads

    def run(self):
        while self.running and self.__time>0:
            time.sleep(1)
            print("remain time: "+str(self.__time)+"s")
            self.__time=self.__time-1
        for i in self.__threads:
            i.stop()
        for i in self.__threads:
            i.join()

    def stop(self):
        self.running=False



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
    sw=SpiderWb(start_user="5822209475",task_name="fans2",threads=4,direction="fans",spider_depth=3,spider_width=20,spider_people=500,spider_time=500,proxy=proxy)
    sw.start()
    sw.time_over()
    #GetInfo("fans.csv").getAll()