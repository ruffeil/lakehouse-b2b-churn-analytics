"""
04_paineis.py — gera os painéis dos slides a partir dos RESULTADOS REAIS da execução
(data/dq_relatorio.json, data/gold/*). Dados sintéticos: ilustram o fluxo, não medem uma empresa.
"""
import json
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, Rectangle
import pandas as pd

GREEN, GREEN_D, DARK, INK = "#00E88F", "#06A869", "#0E1116", "#1A2029"
GRAY, GRAY2, MUTED, WHITE, CORAL, TEAL = "#C9D2DA", "#E8EDF1", "#6B7682", "#FFFFFF", "#FF644E", "#16C8C0"
W, H = 12.0, 16.0

def br(n, d=0):  # número no padrão pt-BR
    s = f"{n:,.{d}f}"; return s.replace(",", "X").replace(".", ",").replace("X", ".")
def mi(n): return "R$ " + br(n / 1e6, 1) + " mi"

def newfig(bg):
    fig = plt.figure(figsize=(W, H), dpi=100)
    ax = fig.add_axes([0, 0, 1, 1]); ax.set_xlim(0, 100); ax.set_ylim(0, 100); ax.axis("off")
    ax.add_patch(Rectangle((0, 0), 100, 100, facecolor=bg, edgecolor="none"))
    return fig, ax
def rrect(ax, x, y, w, h, fc, ec="none", lw=0, rad=1.6):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle=f"round,pad=0,rounding_size={rad}",
                                facecolor=fc, edgecolor=ec, linewidth=lw, mutation_aspect=H / W))

dq = json.load(open("data/dq_relatorio.json"))
res = json.load(open("data/gold/resultados.json"))
seg = pd.read_csv("data/gold/segmentos.csv", index_col=0)
imp = pd.read_csv("data/gold/importancia_variaveis.csv", index_col=0)["queda_auc"]
mc = pd.read_parquet("data/gold/margem_canal_mes")

# ============ PAINEL 1: código + Data Quality (fundo branco) ============
fig, ax = newfig(WHITE)
ax.text(50, 96, "PIPELINE EM EXECUÇÃO", ha="center", fontsize=24, fontweight="bold", color=INK)
ax.text(50, 92.6, "Apache Spark · Bronze → Silver → Gold", ha="center", fontsize=14, color=MUTED)
rrect(ax, 4, 52, 92, 37, DARK, rad=1.4)
code = [
    ("# BRONZE — streaming (em produção: format('kafka'))", MUTED),
    ("spark.readStream.schema(schema).json(raw)", WHITE),
    ("     .writeStream.trigger(availableNow=True)", WHITE),
    ("", WHITE),
    ("# SILVER — regras de Data Quality + quarentena", MUTED),
    ("regras = {'chave_nula': col('cliente_id').isNull(),", GREEN),
    ("          'valor_invalido': col('valor') <= 0,", GREEN),
    ("          'data_invalida': col('dt').isNull()}", GREEN),
    ("w = Window.partitionBy('pedido_id')", WHITE),
    ("          .orderBy(col('_ingest_ts').desc())", WHITE),
    ("silver = bronze.withColumn('rn', row_number().over(w))", WHITE),
    ("", WHITE),
    ("# GOLD — features sem vazamento (só até o corte)", MUTED),
    ("hist = ped.filter(col('dt') <= corte)", WHITE),
    ("feat = hist.groupBy('cliente_id').agg(recencia, freq, desconto...)", WHITE),
]
for i, (t, c) in enumerate(code):
    ax.text(6, 86.2 - i * 2.2, t, fontsize=11.6, family="DejaVu Sans Mono", color=c, va="center")

ax.text(50, 47.5, "DATA QUALITY — RESULTADO REAL DA EXECUÇÃO", ha="center", fontsize=16, fontweight="bold", color=INK)
steps = [("Bronze (bruto)", dq["linhas_bronze"], GRAY), ("− Duplicados", -dq["duplicados_removidos"], CORAL),
         ("− Quarentena", -dq["rejeitados_quarentena"], CORAL), ("Silver (válido)", dq["linhas_silver"], GREEN)]
y = 41
for nome, v, col in steps:
    wbar = 56 * abs(v) / dq["linhas_bronze"] if v > 0 else max(3.5, 56 * abs(v) * 8 / dq["linhas_bronze"])
    rrect(ax, 30, y, wbar, 4.2, col, rad=0.9)
    ax.text(28, y + 2.1, nome, ha="right", va="center", fontsize=13.5, color=INK, fontweight="bold")
    ax.text(30 + wbar + 1.5, y + 2.1, ("+" if v > 0 else "") + br(v), va="center", fontsize=13.5, color=INK)
    y -= 6.2
ax.text(50, 14.5, f"{br(dq['taxa_aprovacao_pct'], 1)}% dos registros aprovados", ha="center", fontsize=22, fontweight="bold", color=GREEN_D)
viol = dq["violacoes_por_regra"]
ax.text(50, 9.2, "Violações: " + " · ".join(f"{k.replace('_', ' ')} {br(v)}" for k, v in viol.items()),
        ha="center", fontsize=10.8, color=MUTED, wrap=True)
ax.text(50, 4.4, "Dados sintéticos — demonstração da arquitetura", ha="center", fontsize=11, color=MUTED, style="italic")
fig.savefig("painel_pipeline.png", dpi=100); plt.close(fig)

# ============ PAINEL 2: dashboard (fundo branco) ============
fig, ax = newfig(WHITE)
ax.text(50, 96, "DASHBOARD EXECUTIVO", ha="center", fontsize=24, fontweight="bold", color=INK)
k = res["kpis"]
tiles = [("Margem 12 meses", mi(k["margem_12m_total_rs"]), INK, GREEN),
         ("Margem em risco", mi(k["margem_em_risco_rs"]), INK, CORAL),
         ("Contas de alto risco", br(k["contas_alto_risco"]), INK, CORAL)]
for i, (t, v, c1, c2) in enumerate(tiles):
    x = 4 + i * 31.5
    rrect(ax, x, 84, 29, 8, GRAY2, rad=1.2)
    ax.text(x + 14.5, 90, t, ha="center", fontsize=12, color=MUTED)
    ax.text(x + 14.5, 86.3, v, ha="center", fontsize=20, fontweight="bold", color=c2 if c2 != GREEN else GREEN_D)

# margem por canal por mês (últimos 12 meses)
a1 = fig.add_axes([0.10, 0.50, 0.85, 0.29])
mc["mes"] = mc["mes"].astype(str); meses = sorted(mc["mes"].unique())[-12:]
cores = {"Representante": INK, "Distribuidor": TEAL, "E-commerce B2B": GREEN_D}
for canal, g in mc.groupby("canal"):
    g = g[g["mes"].isin(meses)].set_index("mes").reindex(meses)
    a1.plot(range(len(meses)), g["margem_rs"] / 1e3, color=cores[canal], lw=3, label=canal, marker="o", ms=4)
a1.set_xticks(range(0, len(meses), 2)); a1.set_xticklabels([m[2:] for m in meses[::2]], fontsize=11)
a1.set_title("Margem por canal (R$ mil/mês)", fontsize=15, fontweight="bold", color=INK, loc="left")
a1.legend(frameon=False, fontsize=11, ncol=1, loc="upper right")
for s in ["top", "right"]: a1.spines[s].set_visible(False)
a1.tick_params(labelsize=11); a1.grid(axis="y", color=GRAY2)

# margem em risco por segmento
a2 = fig.add_axes([0.30, 0.26, 0.65, 0.17])
sg = seg.sort_values("margem_em_risco")
a2.barh(sg.index, sg["margem_em_risco"] / 1e6, color=[CORAL if "risco" in i else GRAY for i in sg.index])
for i, v in enumerate(sg["margem_em_risco"] / 1e6): a2.text(v + 0.05, i, f"R$ {br(v, 1)} mi", va="center", fontsize=11, color=INK)
a2.set_title("Margem em risco por segmento (R$ mi)", fontsize=15, fontweight="bold", color=INK, loc="left")
a2.set_xlim(0, sg["margem_em_risco"].max() / 1e6 * 1.3)
for s in ["top", "right", "bottom"]: a2.spines[s].set_visible(False)
a2.tick_params(labelsize=11, bottom=False); a2.set_xticks([])

# variáveis que mais explicam o churn
a3 = fig.add_axes([0.30, 0.06, 0.65, 0.14])
top = imp.head(5)[::-1]
nomes = {"recencia_dias": "Recência (dias)", "desconto_3m": "Desconto últimos 3m", "desconto_medio": "Desconto médio",
         "pedidos_total": "Pedidos (total)", "reclamacoes_3m": "Reclamações 3m", "tempo_casa_dias": "Tempo de casa"}
a3.barh([nomes.get(i, i) for i in top.index], top.values, color=GREEN_D)
a3.set_title("O que mais explica o churn (queda de AUC)", fontsize=15, fontweight="bold", color=INK, loc="left")
for s in ["top", "right", "bottom"]: a3.spines[s].set_visible(False)
a3.tick_params(labelsize=11, bottom=False); a3.set_xticks([])
ax.text(50, 1.6, "Dados sintéticos — demonstração", ha="center", fontsize=11, color=MUTED, style="italic")
fig.savefig("painel_dashboard.png", dpi=100); plt.close(fig)

# ============ PAINEL 3: modelo + retorno (fundo escuro) ============
fig, ax = newfig(DARK)
m, s = res["modelo"], res["simulacao_retencao"]
ax.text(50, 95, "MODELO E RETORNO", ha="center", fontsize=24, fontweight="bold", color=WHITE)
ax.text(50, 91.4, "Classificação de churn · simulação de retenção", ha="center", fontsize=14, color=GRAY)
cards = [("AUC", br(m["gradient_boosting"]["auc"], 2)), ("Lift top 10%", br(res["modelo"]["lift_top10pct"], 1) + "×"),
         ("Base de churn", br(m["taxa_base_churn_teste"] * 100, 0) + "%")]
for i, (t, v) in enumerate(cards):
    x = 4 + i * 31.5
    rrect(ax, x, 76, 29, 12, INK, rad=1.4)
    ax.text(x + 14.5, 84.4, v, ha="center", fontsize=30, fontweight="bold", color=GREEN)
    ax.text(x + 14.5, 78.6, t, ha="center", fontsize=13, color=GRAY)
ax.text(50, 71.5, f"Top 10% da carteira ({br(s['contas_abordadas'])} contas) concentra {br(s['pct_da_margem_em_risco_total_coberta'], 0)}% da margem em risco",
        ha="center", fontsize=13.2, color=WHITE)
barras = [("Margem em risco\n(top 10%)", s["margem_esperada_em_risco_rs"], CORAL),
          ("Margem preservada\n(25% de salvamento)", s["margem_preservada_rs"], GREEN),
          ("Custo da ação\n(R$ 400/conta)", s["custo_acao_rs"], GRAY)]
mx = max(b[1] for b in barras)
for i, (t, v, c) in enumerate(barras):
    y = 58 - i * 12
    rrect(ax, 34, y, 44 * v / mx + 0.5, 7, c, rad=1)
    ax.text(32, y + 3.5, t, ha="right", va="center", fontsize=12.6, color=WHITE)
    ax.text(34 + 44 * v / mx + 2, y + 3.5, "R$ " + (br(v / 1e6, 2) + " mi" if v >= 1e6 else br(v / 1e3, 0) + " mil"),
            va="center", fontsize=13, color=WHITE, fontweight="bold")
rrect(ax, 8, 12, 84, 16, INK, rad=1.6)
ax.text(50, 22.2, f"ROI simulado: {br(s['roi'], 1)}× sobre o custo da ação", ha="center", fontsize=21, fontweight="bold", color=GREEN)
ax.text(50, 16.6, f"Retorno líquido estimado: R$ {br(s['retorno_liquido_rs'] / 1e6, 2)} mi", ha="center", fontsize=15, color=WHITE)
ax.text(50, 7.5, "Cenário com premissas explícitas, sobre dados sintéticos.", ha="center", fontsize=11.5, color=GRAY, style="italic")
ax.text(50, 4.2, "O ROI real depende da taxa de salvamento medida em campo.", ha="center", fontsize=11.5, color=GRAY, style="italic")
fig.savefig("painel_retorno.png", dpi=100); plt.close(fig)
print("ok")
