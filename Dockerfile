FROM alpine
MAINTAINER yarntime@163.com

ADD build/bin/hybridjob-controller /usr/local/bin/hybridjob-controller

EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/hybridjob-controller"]
