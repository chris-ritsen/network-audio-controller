
import inspect
from enum import Enum
from termcolor import cprint
from fire.decorators import SetParseFns
# from fire.value_types import VALUE_TYPES
from typing import Any, Type, Callable, Annotated

def FireTyped(func:Callable):
    """
    Ensures that all arguments of the called function are validated against their type annotations,
    and are converted to the correct type.

    The type's constructor will be passed the value from the command line (parsed by Fire, so: str|int|float|bool|tuple|list|dict).

    Decorator that uses Fire's SetParseFn to ensure all annotated arguments are parsed to their type,
    including Enums, and prints helpful errors on failure.
    """

    def parse_arg(value:Annotated[Any, "type of fire VALUE_TYPES"], annotation:Type):
        if value is None:
            return value
        try:
            if hasattr(annotation, 'from_string'):
                # If the annotation has a from_string method, call it with the value
                # Example: datetime.from_string(value)
                return annotation.from_string(value)

            elif isinstance(annotation, type) and issubclass(annotation, Enum):
                # If the annotation is an Enum, convert the value to the Enum member
                # Try to get from key first
                try:
                    return annotation[value]
                except KeyError:
                    raise ValueError(f"Invalid Enum key name: {value}")

            elif isinstance(annotation, type):
                return annotation(value)
            else:
                return value

        except ValueError as e:
            import sys
            print(f"Could not convert value '{value}' to {annotation.__name__}:")
            cprint(e, "red")

            if isinstance(annotation, type) and issubclass(annotation, Enum):
                cprint("Valid options:", "yellow")
                for option in annotation:
                    cprint(f"  {option.name}: {option.value}", "yellow")

            # Exit gracefully
            sys.exit(64) # from sysexits.h - EX_USAGE 
        except Exception as e:
            cprint(f"Unexpected error occurred while converting value '{value}' to {annotation.__name__}: {e}", "red")
            raise e

    sig = inspect.signature(func)
    named_type_map = {}
    for name, param in sig.parameters.items():
        annotation = param.annotation
        if annotation != inspect.Parameter.empty:
            # Capture the value of the lambda arguments with = to ensure they're passed correctly to parse_arg
            named_type_map[name] = lambda v, annotation=annotation: parse_arg(v, annotation)

    # Use the built-in SetParseFns decorator
    decorator = SetParseFns(**named_type_map)
    return decorator(func)