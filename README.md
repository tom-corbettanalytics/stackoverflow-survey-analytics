# Set Up

The instructions below are for running the analysis on Ubuntu 18. 

Install Postgres and Python.

```
sudo apt-get update
sudo apt-get install git libpq-dev python-dev postgresql postgresql-contrib
```

Create a new postgres database and user.

```
sudo -u postgres createdb stack_overflow_surveys
sudo -u postgres createuser stack_overflow_analysis --pwprompt
```

Install DBT and python helpers.

```
pip3 install dbt bs4
```

Clone this project.

```
git clone ...
cd ...
```

Update your `~/.dbt/profiles.yml` to include a reference to the Postgres database. Don't forget to include the password you chose when creating the postgres user.

```yaml
default:
  outputs:
    stack_overflow_surveys:
      type: postgres
      threads: 2
      host: 127.0.0.1
      port: 5432
      user: stack_overflow_analysis
      pass: {{ YOUR_PASSWORD_HERE }}
      dbname: stack_overflow_surveys
      schema: public
  target: stack_overflow_surveys
```

