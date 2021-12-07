import secrets
from typing import List

from pydantic import AnyHttpUrl, BaseSettings


class Settings(BaseSettings):
    API_V1_STR: str = ""
    SECRET_KEY: str = secrets.token_urlsafe(32)
    # 60 minutes * 24 hours * 8 days = 8 days
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24 * 8
    # SERVER_NAME: str
    # SERVER_HOST: AnyHttpUrl
    # BACKEND_CORS_ORIGINS is a JSON-formatted list of origins
    # e.g: '["http://localhost", "http://localhost:4200", "http://localhost:3000", \
    # "http://localhost:8080", "http://local.dockertoolbox.tiangolo.com"]'
    BACKEND_CORS_ORIGINS: List[AnyHttpUrl] = []

    ALGORITHM = "HS256"

    STORAGE_ENGINE: str = "mongo"

    PROJECT_NAME: str = "local-test"

    MONGO_HOST: str = "localhost"
    MONGO_PORT: int = 27017
    MONGO_DATABASE: str = "watchmen"
    MONGO_USERNAME: str = None
    MONGO_PASSWORD: str = None

    PRESTO_HOST = "localhost"
    PRESTO_PORT = 8088
    PRESTO_USER = "the_user"
    PRESTO_CATALOG = "mysql"
    PRESTO_SCHEMA = "watchmen"

    MYSQL_HOST: str = ""
    MYSQL_PORT: int = 3306
    MYSQL_USER: str = ""
    MYSQL_PASSWORD: str = ""
    MYSQL_DATABASE: str = "watchmen"
    MYSQL_POOL_MAXCONNECTIONS: int = 6
    MYSQL_POOL_MINCACHED = 2
    MYSQL_POOL_MAXCACHED = 5
    MYSQL_ECHO = False

    ORACLE_LIB_DIR: str = ""
    ORACLE_HOST: str = ""
    ORACLE_PORT: int = 1521
    ORACLE_USER: str = ""
    ORACLE_PASSWORD: str = ""
    ORACLE_SERVICE: str = ""
    ORACLE_SID: str = ""

    JOB_FLAG: bool = False
    WATCHMEN_NAME: str = "local"
    WATCHMEN_HOST: str = "http://localhost:8000/"
    WATCHMEN_PAT: str = "kUWBTfFL_rclOQ0r7_IRDA"
    #
    DATAFRAME_TYPE:str="pandas"
    JOB_TRIGGER:str = "cron"
    DAILY_DAY_OF_WEEK:str ="mon-sun"
    DAILY_HOURS:int = 23
    DAILY_MINUTES:int=59
    WEEKLY_DAY_OF_WEEK:str ="mon"
    WEEKLY_HOURS:int=23
    WEEKLY_MINUTES:int=59
    MONTHLY_DAY:str = "1"
    MONTHLY_HOURS:int=23
    MONTHLY_MINUTES:int=59




    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'
        case_sensitive = True

    # POSTGRES_SERVER: str
    # POSTGRES_USER: str
    # POSTGRES_PASSWORD: str
    # POSTGRES_DB: str
    # SQLALCHEMY_DATABASE_URI: Optional[PostgresDsn] = None
    #
    # @validator("SQLALCHEMY_DATABASE_URI", pre=True)
    # def assemble_db_connection(cls, v: Optional[str], values: Dict[str, Any]) -> Any:
    #     if isinstance(v, str):
    #         return v
    #     return PostgresDsn.build(
    #         scheme="postgresql",
    #         user=values.get("POSTGRES_USER"),
    #         password=values.get("POSTGRES_PASSWORD"),
    #         host=values.get("POSTGRES_SERVER"),
    #         path=f"/{values.get('POSTGRES_DB') or ''}",
    #     )

    # SMTP_TLS: bool = True
    # SMTP_PORT: Optional[int] = None
    # SMTP_HOST: Optional[str] = None
    # SMTP_USER: Optional[str] = None
    # SMTP_PASSWORD: Optional[str] = None
    # EMAILS_FROM_EMAIL: Optional[EmailStr] = None
    # EMAILS_FROM_NAME: Optional[str] = None
    #
    # @validator("EMAILS_FROM_NAME")
    # def get_project_name(cls, v: Optional[str], values: Dict[str, Any]) -> str:
    #     if not v:
    #         return values["PROJECT_NAME"]
    #     return v
    #
    # EMAIL_RESET_TOKEN_EXPIRE_HOURS: int = 48
    # EMAIL_TEMPLATES_DIR: str = "/app/app/email-templates/build"
    # EMAILS_ENABLED: bool = False
    #
    # @validator("EMAILS_ENABLED", pre=True)
    # def get_emails_enabled(cls, v: bool, values: Dict[str, Any]) -> bool:
    #     return bool(
    #         values.get("SMTP_HOST")
    #         and values.get("SMTP_PORT")
    #         and values.get("EMAILS_FROM_EMAIL")
    #     )
    #
    # EMAIL_TEST_USER: EmailStr = "test@example.com"  # type: ignore
    # FIRST_SUPERUSER: EmailStr
    # FIRST_SUPERUSER_PASSWORD: str
    # USERS_OPEN_REGISTRATION: bool = False
    #


settings = Settings()
