# Databricks notebook source
import matplotlib.pyplot as plt

# Dados de exemplo (substitua pelos seus dados reais)
meses = ['Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho', 'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro']
crescimento_economico = [2, 1.5, 3, 0.5, 1, 2.5, 1, 1.8, 2, 1.5, 2.2, 1.7]  # Taxa de crescimento em porcentagem
taxa_emprego = [12, 11, 11.5, 12.5, 13, 13.2, 13.5, 13, 12.8, 12.5, 12, 11.7]  # Taxa de emprego/desemprego em porcentagem

# Criar o gráfico de barras
plt.figure(figsize=(10, 6))

plt.bar(meses, crescimento_economico, alpha=0.7, color='blue', label='Crescimento Econômico')
plt.bar(meses, taxa_emprego, alpha=0.7, color='red', label='Taxa de Emprego/Desemprego')

plt.xlabel('Meses de 2023')
plt.ylabel('Taxa (%)')
plt.title('Taxa de Crescimento Econômico e Taxa de Emprego/Desemprego por Mês em 2023')
plt.legend()

plt.xticks(rotation=45)  # Rotaciona os rótulos do eixo x para facilitar a leitura

plt.tight_layout()
plt.show()


# COMMAND ----------

