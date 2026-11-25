"""
01_gerar_dados.py
Gera dados SINTÉTICOS de uma operação B2B multicanal (ERP em batch + eventos de CRM).
Os dados são sujos de propósito (duplicidades, nulos, valores inválidos) para exercitar o Data Quality.
Nada aqui é dado real de empresa: serve para demonstrar a arquitetura e o fluxo analítico.
"""
import json, os
import numpy as np
import pandas as pd

rng = np.random.default_rng(42)
OUT = "data/raw"
os.makedirs(f"{OUT}/crm_events", exist_ok=True)

N_CLIENTES = 2500
MESES = pd.period_range("2024-01", "2025-12", freq="M")      # 24 meses
CANAIS = ["Representante", "Distribuidor", "E-commerce B2B"]
P_CANAL = [0.45, 0.35, 0.20]
DESC_CANAL = {"Representante": 0.12, "Distribuidor": 0.09, "E-commerce B2B": 0.04}   # desconto médio
MARGEM_CANAL = {"Representante": 0.30, "Distribuidor": 0.26, "E-commerce B2B": 0.34}

# ---------- cadastro de clientes (ERP) ----------
cli = pd.DataFrame({
    "cliente_id": [f"C{i:05d}" for i in range(N_CLIENTES)],
    "canal": rng.choice(CANAIS, N_CLIENTES, p=P_CANAL),
    "porte": rng.choice(["Médio", "Grande"], N_CLIENTES, p=[0.65, 0.35]),
    "uf": rng.choice(["SP", "MG", "RJ", "PR", "RS", "CE", "BA"], N_CLIENTES),
})
cli["ticket_base"] = np.where(cli.porte == "Grande", rng.lognormal(9.4, 0.4, N_CLIENTES), rng.lognormal(8.6, 0.4, N_CLIENTES))
cli["freq_base"] = rng.uniform(0.35, 0.95, N_CLIENTES)          # prob. de pedido por mês
cli["desc_base"] = cli.canal.map(DESC_CANAL) * rng.uniform(0.6, 1.5, N_CLIENTES)

# churn latente: ~22% dos clientes abandonam; a queda de frequência começa antes (churn silencioso)
churn = rng.random(N_CLIENTES) < 0.22
mes_churn = np.where(churn, rng.integers(14, 24, N_CLIENTES), 99)   # índice do mês em que para de comprar
# 30% dos churns acontecem sem sinal prévio (ruído irredutível)
sem_sinal = churn & (rng.random(N_CLIENTES) < 0.30)
cli["mes_churn"], cli["sem_sinal"] = mes_churn, sem_sinal

# ---------- pedidos (ERP, batch) ----------
linhas = []
for r in cli.itertuples():
    for m, per in enumerate(MESES):
        if m >= r.mes_churn:
            break
        meses_para_churn = r.mes_churn - m
        decaimento = 1.0
        if r.mes_churn < 99 and not r.sem_sinal and meses_para_churn <= 5:
            decaimento = 0.25 + 0.15 * meses_para_churn        # frequência cai nos 5 meses antes
        if rng.random() < r.freq_base * decaimento:
            n_ped = 1 + rng.poisson(0.4)
            for _ in range(n_ped):
                valor = r.ticket_base * rng.lognormal(0, 0.35)
                desc = np.clip(r.desc_base + rng.normal(0, 0.03), 0, 0.45)
                if r.mes_churn < 99 and not r.sem_sinal and meses_para_churn <= 5:
                    desc = min(0.45, desc + 0.05)                # desconto sobe tentando segurar a conta
                dia = int(rng.integers(1, 28))
                linhas.append((r.cliente_id, r.canal, f"{per.year}-{per.month:02d}-{dia:02d}",
                               round(valor, 2), round(desc, 4),
                               round(valor * (1 - desc) * (MARGEM_CANAL[r.canal] + rng.normal(0, 0.02)), 2)))
ped = pd.DataFrame(linhas, columns=["cliente_id", "canal", "data_pedido", "valor_bruto", "desconto_pct", "margem_rs"])
ped.insert(0, "pedido_id", [f"P{i:07d}" for i in range(len(ped))])

# ---- sujeira controlada (para o Data Quality encontrar) ----
dup = ped.sample(frac=0.012, random_state=1)                           # pedidos duplicados
ped = pd.concat([ped, dup], ignore_index=True)
idx = ped.sample(frac=0.008, random_state=2).index; ped.loc[idx, "valor_bruto"] = -ped.loc[idx, "valor_bruto"].abs()   # valor negativo
idx = ped.sample(frac=0.006, random_state=3).index; ped.loc[idx, "cliente_id"] = None                                  # chave nula
idx = ped.sample(frac=0.005, random_state=4).index; ped.loc[idx, "data_pedido"] = "31/02/2025"                         # data inválida
idx = ped.sample(frac=0.004, random_state=5).index; ped.loc[idx, "desconto_pct"] = 1.7                                 # desconto fora da faixa
ped = ped.sample(frac=1, random_state=7).reset_index(drop=True)
ped.to_csv(f"{OUT}/erp_pedidos.csv", index=False)
cli.drop(columns=["mes_churn", "sem_sinal"]).to_csv(f"{OUT}/erp_clientes.csv", index=False)

# ---------- eventos de CRM (streaming simulado: micro-lotes JSON) ----------
ev = []
for r in cli.itertuples():
    for m, per in enumerate(MESES):
        if m >= r.mes_churn + 1:
            break
        meses_para_churn = r.mes_churn - m
        base = 0.25
        if r.mes_churn < 99 and not r.sem_sinal and meses_para_churn <= 5:
            base = 0.9                                          # mais chamados/reclamações antes de sair
        for _ in range(rng.poisson(base)):
            ev.append({"cliente_id": r.cliente_id,
                       "tipo": rng.choice(["chamado_suporte", "reclamacao", "cotacao", "visita"], p=[0.4, 0.15, 0.3, 0.15]),
                       "data_evento": f"{per.year}-{per.month:02d}-{int(rng.integers(1, 28)):02d}"})
ev = pd.DataFrame(ev).sample(frac=1, random_state=9).reset_index(drop=True)
for i, parte in enumerate(np.array_split(np.arange(len(ev)), 12)):                 # 12 micro-lotes = 12 "mensagens" do tópico
    with open(f"{OUT}/crm_events/lote_{i:02d}.json", "w") as f:
        for rec in ev.iloc[parte].to_dict("records"):
            f.write(json.dumps(rec) + "\n")

print(f"clientes={len(cli)} pedidos={len(ped)} eventos_crm={len(ev)}")
