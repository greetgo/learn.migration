# Миграция данных (greetgo best practice)

Пример реализации миграции данных в реляционную БД.

Теоретическая часть: https://youtu.be/bycYAMW484c

Для примера выбрана БД PostgreSQL.

Для запуска примера необходимо установить:
 
 - git
 - JDK 8
 - gradle 3.5+ (https://gradle.org/)
 - PostgreSQL Server 9.6+

Если используется Ubuntu, то:
 
 - установка PostgreSQL описана хорошо здесь: 
   https://askubuntu.com/questions/831292/how-to-install-postgresql-9-6-on-any-ubuntu-version

 - установка JDK 8 описана хорошо здесь:
   https://tecadmin.net/install-oracle-java-8-ubuntu-via-ppa/
 
 - ну и установка gradle хорошо описана здесь : https://gradle.org/install
 
 - sudo apt-get install git 

Корректную установку ПО можно проверить запуском в консоле команды:

```
$ gradle -version
```

Должно оттобразиться что-то подобное:

```

------------------------------------------------------------
Gradle 3.5
------------------------------------------------------------

Build time:   2017-04-10 13:37:25 UTC
Revision:     b762622a185d59ce0cfc9cbc6ab5dd22469e18a6

Groovy:       2.4.10
Ant:          Apache Ant(TM) version 1.9.6 compiled on June 29 2015
JVM:          1.8.0_131 (Oracle Corporation 25.131-b11)
OS:           Linux 4.4.0-75-generic amd64

```

Внимание стоит обратить на версии grade и JVM: gradle --> 3.5+ ; JVM --> 1.8.+

Необходимо настроить доступ к БД PostgreSQL под пользователем postgres. Проверить доступ можно командой:

```
$ psql -U postgres -W -h localhost
```

После установки всего необходимого ПО нужно склонировать проект из гитхаба командой:

```
$ git clone https://github.com/john-kolpakov-x/learn.migration.git
```

и зайти в директорию

```
$ cd learn.migration
```

В этой директории будет скрипт prepare_all_databases.sh

В этом скрипте необходимо прописать параметры доступа к БД

```
export PG_ADMIN_USERID=postgres
export PG_ADMIN_PASSWORD=Super_Secret
export PG_ADMIN_URL=jdbc:postgresql://localhost/postgres
```

После чего запустить скрипт командой:

```
$ sh prepare_all_databases.sh
```

Если скрипт отработан корректно, то вылетит в конце сообщение:

```
....

BUILD SUCCESSFUL

Total time: 2.035 secs
```

Далее, для генерации исходных данных для миграции используется команда

```
$ gradle generateCiaData
```

Для запуска самой миграции можно использовать команду

```
$ gradle launchMigration
```
