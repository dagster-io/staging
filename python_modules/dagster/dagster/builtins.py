import typing


class BuiltinEnum:

    ANY = typing.Any  # type: ignore
    BOOL = typing.NewType("Bool", bool)  # type: ignore
    FLOAT = typing.NewType("Float", float)  # type: ignore
    INT = typing.NewType("Int", int)  # type: ignore
    STRING = typing.NewType("String", str)  # type: ignore
    NOTHING = typing.NewType("Nothing", None)  # type: ignore

    @classmethod
    def contains(cls, value):
        for ttype in [cls.ANY, cls.BOOL, cls.FLOAT, cls.INT, cls.STRING, cls.NOTHING]:
            if value == ttype:
                return True

        return False


Any = BuiltinEnum.ANY
String = BuiltinEnum.STRING
Int = BuiltinEnum.INT
Bool = BuiltinEnum.BOOL
Float = BuiltinEnum.FLOAT
Nothing = BuiltinEnum.NOTHING
