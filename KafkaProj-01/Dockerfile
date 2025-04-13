FROM postgres

RUN apt-get update && apt-get install -y sudo
RUN adduser --disabled-password --gecos "" user  \
         && echo 'user:user' | chpasswd \
         && adduser user sudo \
         && echo 'user ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

CMD ["postgres"]