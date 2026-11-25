"""
03_modelo_churn.py
Camada de Analytics sobre a Gold: classificação de churn, LTV em risco, segmentação da carteira
e simulação de retorno da ação de retenção (premissas explícitas).
"""
import json
import numpy as np, pandas as pd
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, average_precision_score
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import make_pipeline
from sklearn.inspection import permutation_importance

df = pd.read_parquet("data/gold/features_cliente")
FEATS = ["recencia_dias", "pedidos_total", "pedidos_3m", "pedidos_3m_ant", "tendencia_pedidos",
         "receita_12m", "margem_12m", "desconto_medio", "desconto_3m", "tempo_casa_dias",
         "chamados_3m", "reclamacoes_3m", "cotacoes_3m"]
X, y = df[FEATS], df["churn"]
Xtr, Xte, ytr, yte = train_test_split(X, y, test_size=0.3, stratify=y, random_state=7)

base = make_pipeline(StandardScaler(), LogisticRegression(max_iter=1000)).fit(Xtr, ytr)
gb = GradientBoostingClassifier(n_estimators=250, max_depth=3, learning_rate=0.05, subsample=0.8, random_state=7).fit(Xtr, ytr)

def prec_topk(y_true, p, k=0.10):
    n = max(1, int(len(p) * k)); top = np.argsort(-p)[:n]
    return float(np.mean(np.asarray(y_true)[top]))

res = {}
for nome, m in [("regressao_logistica", base), ("gradient_boosting", gb)]:
    p = m.predict_proba(Xte)[:, 1]
    res[nome] = {"auc": round(roc_auc_score(yte, p), 3), "pr_auc": round(average_precision_score(yte, p), 3),
                 "precisao_top10pct": round(prec_topk(yte, p), 3)}
res["taxa_base_churn_teste"] = round(float(yte.mean()), 3)
res["lift_top10pct"] = round(res["gradient_boosting"]["precisao_top10pct"] / res["taxa_base_churn_teste"], 2)

pi = permutation_importance(gb, Xte, yte, scoring="roc_auc", n_repeats=8, random_state=7)
imp = pd.Series(pi.importances_mean, index=FEATS).sort_values(ascending=False)
imp.to_csv("data/gold/importancia_variaveis.csv", header=["queda_auc"])

# ---- escoragem de toda a carteira + LTV em risco ----
df["p_churn"] = gb.predict_proba(X)[:, 1]
df["margem_em_risco_12m"] = df["margem_12m"] * df["p_churn"]          # margem esperada que se perde se nada for feito
df["ltv_12m_esperado"] = df["margem_12m"] * (1 - df["p_churn"])       # margem esperada retida nos próximos 12 meses

alto_risco = df["p_churn"] >= df["p_churn"].quantile(0.75)
alto_valor = df["margem_12m"] >= df["margem_12m"].median()
df["segmento"] = np.select(
    [alto_risco & alto_valor, alto_risco & ~alto_valor, ~alto_risco & alto_valor],
    ["Alto valor em risco", "Baixo valor em risco", "Alto valor estável"], default="Estável / baixo valor")
seg = df.groupby("segmento").agg(clientes=("cliente_id", "count"), margem_12m=("margem_12m", "sum"),
                                 margem_em_risco=("margem_em_risco_12m", "sum"), p_churn_medio=("p_churn", "mean")).round(2)
seg.to_csv("data/gold/segmentos.csv")

# ---- simulação de retorno (PREMISSAS, não medição) ----
PREMISSAS = {"abordagem_top_pct_risco_x_valor": 0.10, "taxa_de_salvamento": 0.25, "custo_por_conta_abordada_rs": 400.0}
df["prioridade"] = df["margem_em_risco_12m"]
alvo = df.sort_values("prioridade", ascending=False).head(int(len(df) * PREMISSAS["abordagem_top_pct_risco_x_valor"]))
margem_em_jogo = float(alvo["margem_em_risco_12m"].sum())
margem_salva = margem_em_jogo * PREMISSAS["taxa_de_salvamento"]
custo = len(alvo) * PREMISSAS["custo_por_conta_abordada_rs"]
sim = {"premissas": PREMISSAS, "contas_abordadas": int(len(alvo)), "margem_esperada_em_risco_rs": round(margem_em_jogo),
       "margem_preservada_rs": round(margem_salva), "custo_acao_rs": round(custo),
       "retorno_liquido_rs": round(margem_salva - custo), "roi": round((margem_salva - custo) / custo, 2),
       "pct_da_margem_em_risco_total_coberta": round(100 * margem_em_jogo / df["margem_em_risco_12m"].sum(), 1)}

kpis = {"clientes_escorados": int(len(df)), "margem_12m_total_rs": round(float(df["margem_12m"].sum())),
        "margem_em_risco_rs": round(float(df["margem_em_risco_12m"].sum())),
        "contas_alto_risco": int(alto_risco.sum())}
json.dump({"modelo": res, "kpis": kpis, "simulacao_retencao": sim}, open("data/gold/resultados.json", "w"), indent=2, ensure_ascii=False)
df.to_parquet("data/gold/carteira_escorada.parquet")
print(json.dumps({"modelo": res, "kpis": kpis, "simulacao_retencao": sim}, indent=2, ensure_ascii=False))
print(imp.head(6).round(4).to_string()); print(seg.to_string())
