FROM debian:stable-slim

RUN apt update \
    && apt install -y git curl wget \
    && git version \
    && apt-get clean autoclean \
    && apt-get autoremove --yes \
    && rm -rf /var/lib/{apt,dpkg,cache,log}/

ENV CLI_VERSION=1.4.0
RUN wget -O- https://github.com/cli/cli/releases/download/v${CLI_VERSION}/gh_${CLI_VERSION}_linux_amd64.tar.gz | tar zx --strip-components=1

COPY CICD_GITHUB.txt ./
ADD gh.bash /usr/bin

RUN pwd
RUN ls
RUN chmod +x /usr/bin/gh.bash
RUN cat CICD_GITHUB.txt
ENTRYPOINT ["/usr/bin/gh.bash"]