how to start airflow container on virtual machine:
```
    2  curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.7.1/docker-compose.yaml'
    3  l
    4  nano docker-compose.yaml 
    5  mkdir -p ./dags ./logs ./plugins ./config
    6  echo -e "AIRFLOW_UID=$(id -u)" > .env
    7  docker compose up airflow-init
    8  # Add Docker's official GPG key:
    9  sudo apt-get update
   10  sudo apt-get install ca-certificates curl gnupg
   11  sudo install -m 0755 -d /etc/apt/keyrings
   12  curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
   13  sudo chmod a+r /etc/apt/keyrings/docker.gpg
   14  # Add the repository to Apt sources:
   15  echo   "deb [arch="$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
   16    "$(. /etc/os-release && echo "$VERSION_CODENAME")" stable" |   sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
   17  sudo apt-get update
   18  sudo apt-get install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
   19  sudo groupadd docker
   20  sudo usermod -aG docker $USER
   21  newgrp docker
   22  docker ps
   23  docker compose up airflow-init
   24  docker ps
   25  docker compose up -d
   26  docker ps
   27  nano docker-compose.yaml 
   28  ls .ssh
   29  ssh-keygen -t rsa
   30  cat .ssh/id_rsa.pub 
   31  ls
   32  cd dags/
   33  l
   34  docker ps
```


настройка внутри юи (админ/коннекшнс):

![image](https://github.com/MixKup/MLOps_study/assets/19960794/676c8121-77d8-45c8-91ca-bb16fe4cb9cf)
