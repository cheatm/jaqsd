FROM registry.docker-cn.com/library/python:3.6

RUN apt-get update
RUN apt-get install -y cron libsnappy-dev
RUN echo 'Asia/Shanghai' >/etc/timezone & cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime

ADD ./requirements.txt
RUN pip install -i https://pypi.tuna.tsinghua.edu.cn/simple -r requirements.txt --no-cache-dir

ADD . /jaqsd
WORKDIR /jaqsd

ENV LC_ALL="C.UTF-8" LANG="C.UTF-8" PYTHONPATH=/jaqsd:$PYTHONPATH
RUN ln -s /jaqsd/routing/env.sh /etc/profile.d/env.sh

RUN crontab /jaqsd/routing/timelist

VOLUME "/logs"

CMD /usr/sbin/cron -f


