from model.model.common.user import User


class TokenUser(User):
    token: str = None
