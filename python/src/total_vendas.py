def calcular_total_vendas(quantidades, precos):
    total = sum(q * p for q, p in zip(quantidades, precos))
    return total
