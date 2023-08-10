=============================
Decimal Functions
=============================

.. spark:function:: make_decimal(x, decimal, nullOnOverflow) -> decimal

    Returns decimal value with the type of ``decimal`` from an unscaled bigint value ``x``,
    return null when nullOnOverflow is true, otherwise throw exception.