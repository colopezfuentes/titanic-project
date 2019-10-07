## Titanic Project

### Stack tecnológico
* Sbt
* Scala 2.11.12
* Spark 2.4.3


### Run

```
sbt package
```
```
spark-submit --class Titanic --name Titanic --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.3 /target/scala-2.11/option-project_2.11-0.1.jar
```


### Ejecutar en GCP
## Crear cluster en DataProc
![alt text](cluster-dataproc.png "Logo Title Text 1")
## Crear un job en DataProc
![alt text](job.png "Logo Title Text 1")

## Crear un bucket y subir archivos
![alt text](bucket.png "Logo Title Text 1")

## Finalmente, job exitoso
![alt text](job-success.png "Logo Title Text 1")