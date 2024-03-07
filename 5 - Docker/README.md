# Exemplos de comandos docker
  
  ### Gerenciar Docker

  #### Baixar imagem
  docker pull <nome da image>
  Ex.: docker pull ubuntu

  #### Rodar baixar e rodar imagem
  docker run ubuntu
  ** ATENÇÃO ** : Caso não exista comando que segure a execução da imagem e mesma ira subir e parar imediatamente.

  #### Subir imagem e iniciar terminal interativo
  docker run -it ubuntu bash

  - it -> opção para abrir o comando "bash" de forma interativa após a subida do container / imagem ubuntu (no caso do comando acima)

  #### Definir o nome do conatainer
  docker run -it --name ubuntu01 ubuntu bash

  #### Visualizar as portas mapeadas do container
  docker port <id da image>

  #### Mapear porta entre host e container
  docker --it -p <porta-host>:<porta-container> <imagem> <comando>

  Ex.: docker --it -p 8080:80 ubuntu bash

  #### Visualizar as imagens dispoveis
  docker images

  #### Ispacionar configuração do container
  docker inspect <id-container>

  #### Docker Network
  docker network ls

  #### Criando minha própria imagem
  docker build -t <nomeusuario/nome-imagem>:versão
  Ex.: docker build -t andersonfaro/exemplo-node:0.01 .

  Documentaçãop para entender os principáis comandos para o uso do Dockerfile:
  https://docs.docker.com/reference/dockerfile/

  #### Stop Dockekr
  docker stop <id docker>

  #### Subir imagem para o Docker Hub
  docker push <nome da image>:versão

  #### Criar novo nome / tag para uma imagem existente
  docker tag <imagem origem>:versao <imagem destino>:versao

  #### Tamanho do container
  docker ps -s

  ### Volumes

  #### Mapear caminho no docker
  docker run -it -v <caminho_host>:<caminho_container> imagem comando

  #### Listar volumes docker
  docker volume ls

  #### Criar volumes
  docker volume create <nome-volume>

  #### Montar volume ao iniciar um docker
  docker run -it -v <volume>:<caminho_container> imagem comando

  Ex.: docker run -it -v meu-volume:/app ubuntu bash

  #### Volume temporario no Linux  
  docker run -it --tmpfs=<caminho_container> imagem comando

  Ex.: docker run -it --tmpfs=/app ubuntu bash

  ### Rede

  #### Criar nova rede
  docker network create --driver bridge minha-bridge

  #### Refinir a rede no container
  docker run -it --name <nome-container> --network <nome-rede> <imagem> <comando>

  ##### Exemplo de comunicação de rede
  docker network create minha-bridge

  docker pull aluradocker/alura-books:1.0
  docker pull mongo:4.4.6

  docker run -d --network minha-bridge mongo:4.4.6 --name meu-mongo
  docker run -d --network minha-bridge -p 3000:3000 aluradocker/alura-books:1.0 --name alura-books