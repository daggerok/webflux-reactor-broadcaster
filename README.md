# reactor webflux broadcaster
Easy messages broadcasting (redistribution by publishing / subscribing) with spring-webflux and project-reactor!

## play

```bash
./mvnw spring-boot:run
#http --stream :8080
curl 127.0.0.1:8080

http :8080 body=Hello
http :8080 body=world
http :8080 body=ololo
http :8080 body=trololo

http :8080/last/3
```

## reference

* [Read: reactor processors](https://projectreactor.io/docs/core/release/reference/#processor-overview)

