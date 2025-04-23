# mongo-postgres
- Build docker images

```docker build -t etl_pipeline .```

- Run docker container 

```docker run --env-file config/.env -v ./data:/app/data --network mynetwork --name etl_pipeline etl_pipeline```