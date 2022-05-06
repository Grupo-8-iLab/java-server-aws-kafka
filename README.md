<h1>
  CONSUMER KAFKA
</h1>

## 💻 Sobre o projeto

A Bolinho de Java Corp teve como desafio desenvolver uma aplicação que faça o upload de um arquivo CSV de produtos, o guarde na nuvem(bucket S3) e através do kafka notifique o consumer para que ele recupere o arquivo no bucket S3, o processe a armazene os dados num banco de dados num RDS(Relational Database Service) da AWS.

## 👨🏻‍💻 Desenvolvedores

- [Debora Brum](https://github.com/DeboraBrum)
- [Edvan Jr.](https://github.com/Edvan-Jr)
- [Geraldo Jr.](https://github.com/GeraldinJr)
- [Lucas Paixão](https://github.com/lucasfpds)
- [Magnólia Medeiros](https://github.com/magnoliamedeiros)

## 🚀 Tecnologias

- [Java](https://www.java.com/pt-BR/)
- [Spring](https://spring.io/)
- [PostgreSQL](https://www.postgresql.org/)
- [Amazon EC2](https://aws.amazon.com/pt/ec2/)
- [Kafka](https://aws.amazon.com/pt/msk/)
- [Amazon S3](https://aws.amazon.com/pt/s3/)
- [Amazon RDS](https://aws.amazon.com/pt/rds/)

## ⚙️ Funcionalidades

- Consumir mensagens dos tópicos do Kafka, especificamente os nomes dos arquivos no Bucket S3
- Baixar arquivos do Bucket
- Salvar o conteúdo dos arquivos no RDS

## 📄 Licença

Este projeto está sob a licença de Bolinho de Java Corp.
