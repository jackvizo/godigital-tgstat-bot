# Бизнес требования к собираемым данным

Подписки (пользователи, которые подписались)
1.1 Дата подписки
1.2 Наличие premium у пользователя
1.3 Имя пользователя
1.4 Подписка по ссылке (флаг того, подписался ли пользователь по ссылке или присоединился самостоятельно)

Отписки (пользователи, которые отписались)
2.1 Дата отписки
2.2 Отписки по ссылке (флаг того, был ли изначально пользователь подписан на канал через ссылку или был подписан самостоятельно)
2.3 Имя пользователя

Охваты постов
3.1 За все время
3.2 За первые 24 ч
3.3 За первый час

Комментарии
4.1 Кол-во комментариев
4.2 Кол-во комментариев за первый час

Реакции
5.1 За все время
5.2 За первый час

В рамках данного ТЗ предполагается, что аккаунт, от имени которого будет выполняться скрипт, имеет админ доступ к каналу

# Существующее решение

В нашем распоряжении есть код телеграм бота для сбора данных из Telegram каналов (`ROMAN_PROJECT`).
Код проекта представлен в файле `ROMAN_PROJECT.zip`
Код написан на python с применением библиотеки `Telethon`.
В качества БД используются два сервиса - `MySQL` и `SQLite`.

Бот умеет собирать следующие требуемые данные

- Охваты за всё время
- Кол-во комментариев к посту за всё время
- Реакции за всё время

# Что следует взять из готового кода

- `ROMAN_PROJECT/ca-views.py:run_logging`: точка входа
- `ROMAN_PROJECT/models/tmit.py:read_channels`: Возвращает id канала. На вход принимает строку, которая может являться: неким Id канала (не нашел примера использования), либо человеко читаемым названием диалога, на который подписан аккаунт бота. На данном этапе следует использовать человеко читаемым название диалога.
- `ROMAN_PROJECT/models/tmit.py:get_comments` собирает сущности комментариев и пользователей в комменатиях.
- `ROMAN_PROJECT/models/tmit.py:get_posts`: Собирается статистика о реакциях, количестве комментариев к посту за все время, количестве пользователей прокомментировавших пост за все время, дате публикации. Функция внутри себя сохраняет посты в БД.
- `ROMAN_PROJECT/models/class_models.py`: структуры `User`, `Post`, `Reaction`

# Как следует изменить схему таблиц в БД

В качестве БД будет использована PostgreSQL.
В `ROMAN_PROJECT` следует заменить адаптер к БД на PostgreSQL.

См. полную схему таблиц в `schema.prisma`.
Разработчику предлагается самостоятельно поднять локальную БД в docker контейнере и выполнить миграции. Файл `schema.prisma` приведен в качестве примера и не обязателен для реального применения в качества файла миграции.
Решение по выполнению миграции остается на усмотрение разработчика.

Таблица `User` переименовывается в `stat_user`, поля сущности `ROMAN_PROJECT/models/class_models.py:User` меняются следующим образом

| Старое значение  | Новое значение      |
| ---------------- | ------------------- |
| `id`             | `pk`                |
| `user_id`        | `tg_user_id`        |
| `firstName`      | `first_name`        |
| `lastName`       | `last_name`         |
| `username`       | `user_name`         |
| `isScam`         | `scam`              |
| `isPremium`      | `premium`           |
| `isVerified`     | `verified`          |
| `date_of_update` | `timestamp`         |
| `phone`          | `phone`             |
| `source`         | `tg_channel_id`     |
|                  | `is_joined_by_link` |
|                  | `tg_channel_id`     |
|                  | `joined_at`         |
|                  | `left_at`           |

Таблица `Post` переименовывается в `stat_post`, поля сущности `ROMAN_PROJECT/models/class_models.py:Post` меняются следующим образом

| Старое значение           | Новое значение                |
| ------------------------- | ----------------------------- |
| `id`                      | `pk`                          |
| `date_of_update`          | `timestamp`                   |
| `channel_id`              | `tg_channel_id`               |
| `post_id`                 | `tg_post_id`                  |
| `message`                 | `message`                     |
| `views`                   | `views`                       |
|                           | `views_1h`                    |
|                           | `views_24h`                   |
| `total_reactions_count`   | `total_reactions_count`       |
|                           | `reactions_1h`                |
|                           | `reactions_24h`               |
| `comments_messages_count` | `comments_messages_count`     |
|                           | `comments_messages_count_1h`  |
|                           | `comments_messages_count_24h` |
| `comments_users_count`    | `comments_users_count`        |
| `comments_channels_count` | `comments_channels_count`     |
| `link`                    | `link`                        |
| `media`                   | `media`                       |
| `date_of_post`            | `date_of_post`                |
| `forwards`                | `forwards`                    |

Таблица `Reaction` переименовывается в `stat_reaction`, поля сущности `ROMAN_PROJECT/models/class_models.py:Reaction` меняются следующим образом

| Старое значение          | Новое значение           |
| ------------------------ | ------------------------ |
| `id`                     | `pk`                     |
| `date_of_update`         | `timestamp`              |
| `channel_id`             | `tg_channel_id`          |
| `post_id`                | `tg_post_id`             |
| `reaction_count`         | `reaction_count`         |
| `reaction_emoticon`      | `reaction_emoticon`      |
| `reaction_emoticon_code` | `reaction_emoticon_code` |

# Prefect.io

В качестве фреймворка следует взять `https://prefect.io`

Ниже представлен Quick Start гайд, чтобы начать разработку

1. Создать каталог с проектом
2. Создать виртуальное окружение (если используется PyCharm он это сделает автоматически)

```sh
python -m venv venv
```

3. Установить prefect

```sh
pip install prefect
```

4. Создать `tg_collect_flow.py` - скрипт, который будет агрегировать данные из Telegram API

```python
from prefect import flow, task


@flow(name="tg-collect", log_prints=True, on_crashed=[handle_flow_error],
      on_failure=[handle_flow_error],
      on_cancellation=[handle_flow_error])
def tg_collect_flow():
  tg_channel_name="Человеко читаемое название канала"
  phone_number=79999999999
  # В теле этой функции будет размещена основная логика скрипта, например:
  db = create_db_instance()
  tg_client = task_authorize(db, phone_number)
  task_collect_data(db, tg_client, tg_channel_name)

@task
def task_authorize(db, phone_number):
  pass

@task
def task_collect_data(db, tg_client, tg_channel_name):
  pass
```

5. Создать файл `main.py`

```python
from tg_collect_flow import flow_collect_tg_channels_by_phone_number

if __name__ == '__main__':
    # Скрипт будет запускаться каждые 5 минут
    flow_collect_tg_channels_by_phone_number.serve(name="tg-collect", interval=300)
```

6. Запустить сервер

```sh
prefect server start
```

7. Запустить один раз скрипт

```sh
python main.py
```

8. Запустить скрипт

```sh
python tg_collect_flow.py
```

Перейдите в веб-интерфейс сервера `http://localhost:4200/deployments`.
В панели должна появиться запись с запущенным скриптом `tg-flow`, который будет вызываться каждые 5 минут

Данный пример демонстрирует запуск скрипта только для одного канала. В рамках этого ТЗ этого достаточно.

# Авторизация

Для авторизации бота в TG следует разработать своё решение.

## Модель данных

См. полную схему таблиц в `schema.prisma`

Добавить следующие таблицы в БД

- `config__tg_channel`
- `config__tg_bot_session_pool`

`config__tg_bot_session_pool` хранит настройки подключения бота (`api_id`, `api_hash`, `bot_token`, `phone_number`).
`config__tg_bot_session_pool`.`session_bytes` хранит подтверждённую двухфакторной авторизацией сессию в бинарном формате (файл `session_name.session`, который создает Telethon клиент).
`config__tg_bot_session_pool`.`status` принимает следующие значения: `enabled` | `banned`. По умлочанию `enabled`. Если бота забанило, статус должен измениться на `banned` и такой бот не должен быть задействован в парсинге.

`config__tg_channel` ссылается на пул сессий бота `config__tg_bot_session_pool`

## add_bot_session.py

Скрипт будет запускаться вручную из терминала для авторизации бота.
Скрипт должен получить авторизацию по переданным входным параметрам и сохранить файл авторизации в БД.

Входные параметра скрипта

| Параметр       | Описание                                         |
| -------------- | ------------------------------------------------ |
| `api_id`       | получается при регистрации бота в `telegram.org` |
| `api_hash`     | получается при регистрации бота в `telegram.org` |
| `phone_number` | номер телефона, на который зарегистрирован бот   |

Пример авторизации:

```python
api_id = 123456
api_hash = '111111111111111'
phone = '79999999999'

client = TelegramClient(api_id, api_hash, phone).start()
```

Во время работы скрипта в терминал понадобиться ввести пароль и 2FA код.
После этого создастся файл `session_name.session` в том же каталоге, что и скрипт.
Необходимо прочитать этот файл и сохранить в БД, создав запись
`config__tg_bot_session_pool`.

| Поле                                          | Значение                                                                                      |
| --------------------------------------------- | --------------------------------------------------------------------------------------------- |
| `config__tg_bot_session_pool`.`api_id`        | `args`.`api_id` (`args` - входные параметры скрипта)                                          |
| `config__tg_bot_session_pool`.`api_hash`      | `args`.`api_hash`                                                                             |
| `config__tg_bot_session_pool`.`bot_token`     | `args`.`bot_token`                                                                            |
| `config__tg_bot_session_pool`.`phone_number`  | `args`.`phone_number`                                                                         |
| `config__tg_bot_session_pool`.`session_bytes` | Содержимое файла `session_name.session`, который сохраняет telethon клиент в файловую систему |

Дальнейшая авторизация в `tg_collect_flow.py` скрипте будет выполняться следующим образом

```python
api_id = config__tg_bot_session_pool.api_id
api_hash = config__tg_bot_session_pool.api_hash
session_file_path = read_session_bytes_from_db_to_tmp_folder()

client = TelegramClient(session_file_path, api_id, api_hash).start()

get_posts(client, await read_channels(client, channel_name), max_posts=max_posts, parent="")
```

## add_channel.py

Скрипт будет запускаться вручную для добавления канала.
Перед вызовом скрипта необходимо добавить хотя бы одну сессию с помощью `add_bot_session.py`.
Аккаунт должен быть подписан на добавляемый канал.

| Параметр          | Описание                                                      |
| ----------------- | ------------------------------------------------------------- |
| `phone_number`    | номер телефона, на который зарегистрирован бот                |
| `tg_channel_name` | Человеко читаемое имя канала, как оно отображается в диалогах |

Скрипт авторизуется сессией `config__tg_bot_session_pool`.`session_bytes` у которой `config__tg_bot_session_pool`.`phone_number` = `args`.`phone_number` и `config__tg_bot_session_pool`.`status` = `enabled`.
С помощью кода, описанного в проекте партнёров (`models/tmit.py:read_channels`), получаем id телеграм канала и создаём запись `config__tg_channel`

| Поле                                               | Значение                                                                        |
| -------------------------------------------------- | ------------------------------------------------------------------------------- |
| `config__tg_channel`.`tg_channel_id`               | Получить из `models/tmit.py:read_channels`                                      |
| `config__tg_channel`.`tg_channel_name`             | `args`.`tg_channel_name`                                                        |
| `config__tg_channel`.`config__tg_bot_session_pool` | Сущность `config__tg_bot_session_pool`, с помощью которой произошла авторизация |

# Сбор охватов, комментариев и реакций за всё время

В `tg_collect_flow.py` следует перенести код из `ROMAN_PROJECT/ca-views.py:run_logging`, в частности следующую конструкцию

```python
get_posts(client, await read_channels(client, channel_name), max_posts=max_posts, parent="")
```

В тела функций следует внести правки, связаные с изменением схемы таблиц БД и сервиса БД с MySQL/SQLite на PostgreSQL.

# Сбор событий подписок/отписок по ссылке/без ссылки

Информацию о том, что пользователь подписался или отписался, возвращает метод `ROMAN_PROJECT/client.iter_admin_log`.
(Документация Telethon)[https://docs.telethon.dev/en/stable/modules/client.html#telethon.client.chats.ChatMethods.iter_admin_log].
В метод следует передать параметры `join=True`, `leave=True`, `invite=True`.

Ответ метода будет содержать объект соответствия `user_id` -> `User`.

Список пользователей из ответа `client.iter_admin_log` следует обработать в цикле по следующей логике.
Найти в БД последнюю существующую запись где `stat_user`.`tg_channel_id` = `<id текущего канала>` и `stat_user`.`tg_user_id` = `admin_log.user.id` (отсортировка от новых к старым, `desc`, по полю `stat_user`.`timestamp`).
Если запись найдена, обновить её в соответствии с событиями из `admin_log` (см. таблицу ниже)
Если запись в БД не найдена, добавить новую запись в БД.

| Поле                            | Значение                                                                                                         |
| ------------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| `stat_user`.`timestamp`         | Текущая дата и время                                                                                             |
| `stat_user`.`left_at`           | `admin_log`.`date`, если для данного пользователя была найдена запись `admin_log`.`joined`                       |
| `stat_user`.`joined_at`         | `admin_log`.`date`, если для данного пользователя была найдена запись `admin_log`.`left`                         |
| `stat_user`.`is_joined_by_link` | если для данного пользователя была найдена запись `admin_log`.`joined_by_invite` или `admin_log`.`joined_invite` |
| `stat_user`.`tg_channel_id`     | id текущего канала                                                                                               |
| `stat_user`.`tg_user_id`        | `admin_log`.`user`.`id`                                                                                          |
| `stat_user`.`first_name`        | `admin_log`.`user`.`first_name`                                                                                  |
| `stat_user`.`first_last`        | `admin_log`.`user`.`last_name`                                                                                   |
| `stat_user`.`username`          | `admin_log`.`user`.`username`                                                                                    |
| `stat_user`.`scam`              | `admin_log`.`user`.`scam`                                                                                        |
| `stat_user`.`premium`           | `admin_log`.`user`.`premium`                                                                                     |
| `stat_user`.`verified`          | `admin_log`.`user`.`verified`                                                                                    |
| `stat_user`.`phone`             | `admin_log`.`user`.`phone`                                                                                       |

# Учёт охватов, комментариев и реакций за первые 1ч и 24ч

После вызова `ROMAN_PROJECT/models/tmit.py:get_posts` получить из БД последнюю существующую запись `stat_post`, (отсортировка от новых к старым, `desc`, по полю `user`.`timestamp`), где `stat_post`.`tg_channel_id` = `<id текущего канала>`.
Если `stat_post.id` отличается от последнего `id` поста из `ROMAN_PROJECT/models/tmit.py:get_posts`, значит был добавлен новый пост и нужно запланировать запуск скрипта `tg-collect` на +1 час и +24 часа от даты создания нового поста.

Ниже представлен фрагмент кода, как запланировать событие с помощью prefect.io API:

```python
import httpx
from datetime import datetime, timedelta

PREFECT_SERVER_URL = 'http://localhost:4200'

def get_deployment_by_flow_name(flow_name: str):
    response = httpx.post(f"{PREFECT_SERVER_URL}/deployments/filter", json={
        "flows": {
            "name": {
                "any_": [flow_name]
            }
        }
    })
    json = response.json()
    return json[0] if json else None

def create_scheduled_flow_run(deployment_id: str, scheduled_start_time: datetime):
    httpx.post(f"{PREFECT_SERVER_URL}/deployments/{deployment_id}/create_flow_run", json={
        "parameters": {},
        "state": {
            "type": "SCHEDULED",
            "state_details": {
                "scheduled_time": scheduled_start_time.isoformat()
            }
        }
    })

deployment = get_deployment_by_flow_name("tg_collect_flow")
deployment_id = deployment.get('id')
scheduled_start_time = datetime.utcnow() + timedelta(hours=1)
create_scheduled_flow_run(deployment_id, scheduled_start_time)
```

Также следует модифицировать функцию `ROMAN_PROJECT/models/tmit.py:get_comments` и `ROMAN_PROJECT/models/tmit.py:get_posts`
так, чтобы они совершили дополнительный вызов методов `client.iter_messages` и `client(GetRepliesRequest(......))` с выборкой сущностей за 24ч с момента создания поста. Подсчет количества сущностей за 1ч будет выполняться в цикле по полученной выборке.

Данные об обхватах сущностей следует сохранить в следующие поля БД

| Поле                                      | Значение                            |
| ----------------------------------------- | ----------------------------------- |
| `stat_post`.`view_1h`                     | Охваты за 1ч                        |
| `stat_post`.`view_24h`                    | Охваты за 24ч                       |
| `stat_post`.`reactions_1h`                | Количество всех реакций за 1ч       |
| `stat_post`.`reactions_24h`               | Количество всех реакций за 24ч      |
| `stat_post`.`comments_messages_count_1h`  | Количество всех комментариев за 1ч  |
| `stat_post`.`comments_messages_count_24h` | Количество всех комментариев за 24ч |

# Деплой, вывод в продакшн и масштабирование скрипта на множество tg-каналов

В рамках данного ТЗ разработчику не нужно внедрять решение в продакшн и масштабировать решение на несколько каналов.
Ответственным за этот процессы выступает Евгений Рязанцев.

# Если что-то не понятно

Обращайтесь за консультациями в телеграм Евгению Рязанцеву @gaever
