"""Dígito de control de contenedores según ISO 6346."""
from __future__ import annotations

import re

_LETTER_VALUES: dict[str, int] = {}
_value = 10
for _letter in "ABCDEFGHIJKLMNOPQRSTUVWXYZ":
    if _value % 11 == 0:          # se omiten los múltiplos de 11
        _value += 1
    _LETTER_VALUES[_letter] = _value
    _value += 1

CONTAINER_PATTERN = re.compile(r"^[A-Z]{3}[UJZ]\d{7}$")


def check_digit(owner_and_serial: str) -> int:
    """Dígito de control para los 10 primeros caracteres (prefijo de 4 letras y serie de 6 dígitos)."""
    total = sum((_LETTER_VALUES[c] if c.isalpha() else int(c)) * 2 ** i
                for i, c in enumerate(owner_and_serial))
    return total % 11 % 10


def is_valid(container_number: str | None) -> bool:
    if not container_number or not CONTAINER_PATTERN.match(container_number):
        return False
    return check_digit(container_number[:10]) == int(container_number[10])
