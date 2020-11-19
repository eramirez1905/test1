# Data Engineering

## Adding a new environment

To add a new environment, for example, like `mkt`, please follow the following steps:

- Add the alias in the terraform project.
- Create the configuration files in the helm project.
- Create the database in the corresponding RDS instance (staging and production).
  - In the newly created database, make yourself admin.
```sql
update ab_user_role set role_id = 1
where user_id = (select id from ab_user
                 where email = 'EMAIL@deliveryhero.com'
                )
```
- Update the the deployment of environment in `deployment/deploy.sh`.
