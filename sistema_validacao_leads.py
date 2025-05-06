import pandas as pd
import re
import random
import streamlit as st
from io import BytesIO
import pymysql
import requests

# === CONFIGURAÇÕES ===
SDRS = {
    "Diogo Souza": 22709788,
    "Felipe Silva": 23371185,
    "izabelle Vicente": 23371174,
    "Jardielle Gomes": 21123005,
    "Karen Mercini": 23499533,
    "rosemeiresantos": 23371196,
    "Vitória Paixão": 22495673
}

API_TOKEN = st.secrets["API_TOKEN"]
BASE_URL = "https://centraldosbeneficios.pipedrive.com/api/v1"
HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json"
}

DB_CONFIG = {
    "host": st.secrets["DB_CONFIG"]["host"],
    "port": st.secrets["DB_CONFIG"]["port"],
    "user": st.secrets["DB_CONFIG"]["user"],
    "password": st.secrets["DB_CONFIG"]["password"],
    "database": st.secrets["DB_CONFIG"]["database"]
}

CAMPO_BENEFICIOS_NEGOCIACAO = {
    "Plano Odontológico - Metlife": 1284,
    "Plano Odontológico - Odontoprev": 911,
    "Seguro Bem-Estar Integral - Bronze (Creche)": 906,
    "Seguro Bem-Estar Integral - Bronze (Kit natalidade)": 907,
    "Seguro Bem-Estar Integral - Diamante": 908,
    "Seguro Bem-Estar Integral - Ouro": 909,
    "Seguro Bem-Estar Integral - Prata": 919,
    "Seguro Bem-Estar Integral - Safira": 1040,
    "Seguro de Vida": 103
}

CAMPO_BENEFICIOS_CCT = {
    "Nenhum": 299,
    "Plano Odontológico | Metlife": 95,
    "Plano Odontológico | Odontoprev": 693,
    "Seguro Bem-Estar Integral | Bronze Creche": 93,
    "Seguro Bem-Estar Integral | Bronze Kit Natalidade": 694,
    "Seguro Bem-Estar Integral | Diamante": 697,
    "Seguro Bem-Estar Integral | Ouro": 695,
    "Seguro Bem-Estar Integral | Prata": 696,
    "Seguro Bem-Estar Integral | Safira Saúde +": 1088,
    "Seguro de Vida": 94
}

def validar_ou_criar_opcao_enum(campo_key, valor, entidade="organizationFields"):
    try:
        # Buscar todos os campos da entidade
        res = requests.get(f"{BASE_URL}/{entidade}?api_token={API_TOKEN}", headers=HEADERS)
        res.raise_for_status()
        campos = res.json().get("data", [])

        # Encontrar o campo pelo 'key'
        campo = next((c for c in campos if c["key"] == campo_key), None)
        if not campo:
            raise Exception(f"Campo com key '{campo_key}' não encontrado.")

        field_id = campo["id"]
        opcoes = campo.get("options", [])

        # Verifica se já existe a opção
        for opcao in opcoes:
            if opcao["label"].strip().lower() == valor.strip().lower():
                return opcao["id"]

        # Cria nova lista com as opções existentes + nova
        nova_opcao = {"label": valor}
        opcoes_atualizadas = opcoes + [nova_opcao]

        # Atualiza o campo completo com nova lista de opções
        update_payload = {
            "options": [
                {"id": opt["id"], "label": opt["label"]} if "id" in opt else {"label": opt["label"]}
                for opt in opcoes_atualizadas
            ]
        }

        update_res = requests.put(
            f"{BASE_URL}/{entidade}/{field_id}?api_token={API_TOKEN}",
            headers=HEADERS,
            json=update_payload
        )
        update_res.raise_for_status()

        # Buscar o campo novamente para pegar o ID da nova opção
        res_confirm = requests.get(f"{BASE_URL}/{entidade}?api_token={API_TOKEN}", headers=HEADERS)
        res_confirm.raise_for_status()
        campo_atualizado = next((c for c in res_confirm.json().get("data", []) if c["key"] == campo_key), None)

        for opt in campo_atualizado.get("options", []):
            if opt["label"].strip().lower() == valor.strip().lower():
                return opt["id"]

        raise Exception(f"Nova opção '{valor}' não encontrada após atualização.")

    except Exception as e:
        raise Exception(f"Erro ao validar/criar opção '{valor}' no campo '{campo_key}': {e}")


def importar_no_crm(df):
    resultados = []
    for _, row in df.iterrows():
        try:
            user_id = SDRS.get(row.get('Proprietario'))
            beneficios_ids = [CAMPO_BENEFICIOS_CCT[b] for b in row.get('Benefícios previstos em CCT', []) if b in CAMPO_BENEFICIOS_CCT]
            id_categoria = validar_ou_criar_opcao_enum("c5364f522c46028ed0bdc86f22796c6a66caf185", row.get("Categoria", ""))
            id_sindicato = validar_ou_criar_opcao_enum("550c4dddf9965d646ded8c5f3c5a3f6c329107b8", row.get("Sindicato", ""))
            org_payload = {
                "name": str(row.get('Razão', "")),
                "visible_to": "3",
                "address": f"{row.get('Endereço', '')}, {row.get('Número', '')} {row.get('Complemento', '')} - {row.get('Bairro', '')}, {row.get('Cidade', '')} - {row.get('UF', '')} {row.get('CEP', '')}",
                "fe19cbb1fdc5037d95a7ae6fa93a6f09ce93c158": str(row.get('CNPJ', "")),
                "c6dc8be615895b96932a3c0f9de1366a26b4cb3c": str(row.get('CNPJ (CE)', "")),
                "af490c540d3b119809b507f78412b628f4359409": beneficios_ids,
                "c5364f522c46028ed0bdc86f22796c6a66caf185": id_categoria,
                "550c4dddf9965d646ded8c5f3c5a3f6c329107b8": id_sindicato
            }
            org_res = requests.post(f"{BASE_URL}/organizations?api_token={API_TOKEN}", headers=HEADERS, json=org_payload)
            org_res.raise_for_status()
            org_id = org_res.json()['data']['id']
            person_payload = {
                "name": str(row.get('Pessoa', "")),
                "email": str(row.get('Email', "")),
                "phone": str(row.get('Telefone 1', "")),
                "owner_id": user_id,
                "org_id": org_id
            }
            person_res = requests.post(f"{BASE_URL}/persons?api_token={API_TOKEN}", headers=HEADERS, json=person_payload)
            person_res.raise_for_status()
            person_id = person_res.json()['data']['id']
            lead_payload = {
                "title": str(row.get('Razão', "")),
                "person_id": person_id,
                "organization_id": org_id,
                "owner_id": user_id,
                "visible_to": "1"
            }
            lead_res = requests.post(f"{BASE_URL}/leads?api_token={API_TOKEN}", headers=HEADERS, json=lead_payload)
            lead_res.raise_for_status()
            lead_id = lead_res.json().get('data', {}).get('id')
            update_payload = {}
            cadencia = row.get("Cadência Meetime", None)
            if pd.notna(cadencia):
                update_payload["76a9bd6d698a809abc6c6a14b91875014b3d6030"] = cadencia
            beneficio_id = CAMPO_BENEFICIOS_NEGOCIACAO.get(row.get("Benefícios   em negociação", ""), None)
            if pd.notna(beneficio_id):
                update_payload["3ab025f846119bc314260294091215d18f812f24"] = [beneficio_id]
            if update_payload:
                requests.patch(f"{BASE_URL}/leads/{lead_id}?api_token={API_TOKEN}", headers=HEADERS, json=update_payload)
            resultados.append(f"✅ Lead '{row['Razão']}' importado com sucesso.")
        except Exception as e:
            resultados.append(f"❌ Erro ao importar {row.get('Pessoa', '')}: {e}")
    return resultados

def limpar_nome_pessoa(nome):
    if pd.isna(nome):
        return ""
    return nome.split("-")[0].strip()

def remover_mascara_cnpj(cnpj):
    return re.sub(r"\D", "", str(cnpj))

def gerar_emails_unicos(df):
    email_map = {}
    contador_email = {}
    for idx, row in df.iterrows():
        original_email = row['Email']
        if pd.isna(original_email):
            continue
        if original_email not in email_map:
            sdr = list(SDRS.keys())[len(email_map) % len(SDRS)]
            email_map[original_email] = sdr
            contador_email[original_email] = 1
            df.at[idx, 'Email'] = original_email
        else:
            contador_email[original_email] += 1
            nome, dominio = original_email.split('@')
            novo_email = f"{nome}{contador_email[original_email]}@{dominio}"
            df.at[idx, 'Email'] = novo_email
        df.at[idx, 'Proprietario'] = email_map[original_email]
    return df

def distribuir_sdr(df):
    df = df.sample(frac=1).reset_index(drop=True)
    df['Proprietario'] = [list(SDRS.keys())[i % len(SDRS)] for i in range(len(df))]
    return df

def verificar_empresa_cadastrada(cnpj_limpo):
    try:
        connection = pymysql.connect(**DB_CONFIG)
        with connection.cursor() as cursor:
            cursor.execute("SELECT * FROM empresas WHERE CNPJ = %s AND CodigoMPlan IS NOT NULL", (cnpj_limpo,))
            mplan = cursor.fetchone()
            cursor.execute("SELECT * FROM empresas WHERE CNPJ = %s AND CodigoCV IS NOT NULL", (cnpj_limpo,))
            cv = cursor.fetchone()
        connection.close()
        return "SIM" if mplan or cv else "NÃO"
    except Exception:
        return "ERRO"

def tratar_planilha(df, sindicato, categoria, beneficio, cadencia, beneficios_cct):
    df.columns = df.columns.str.strip()
    df = df[df['E-mail'].notna()]
    df = df[df['Quadro de Funcionários'].isin([
        '20 A 99 COLABORADORES', '100 A 500 COLABORADORES',
        'ACIMA DE 500 COLABORADORES', 'ACIMA DE 700 COLABORADORES'])]

    df['Pessoa'] = df['Nome do Sócio'].apply(limpar_nome_pessoa)
    df['Pessoa'] = df.apply(lambda row: f"Desconhecido-{row.name}" if row['Pessoa'] == "" else row['Pessoa'], axis=1)
    df = df.drop_duplicates(subset=['Pessoa'])

    df = distribuir_sdr(df)
    df['Email'] = df['E-mail']
    df = gerar_emails_unicos(df)
    df['Número'] = df['Número'].astype(str)
    df['CNPJ (CE)'] = df['CNPJ'].apply(remover_mascara_cnpj)
    df['Empresa Cadastrada'] = df['CNPJ (CE)'].apply(verificar_empresa_cadastrada)

    df['Razão'] = df['Razão'].astype(str)
    df['CNPJ'] = df['CNPJ'].astype(str)

    df['Sindicato'] = sindicato
    df['Categoria'] = categoria
    df['Benefícios   em negociação'] = beneficio
    df['Cadência Meetime'] = cadencia
    df['Benefícios previstos em CCT'] = [beneficios_cct] * len(df)

    return df

def exportar_excel(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)
    output.seek(0)
    return output

# === INTERFACE STREAMLIT ===
st.set_page_config(page_title="Validador de Mailings", layout="wide")
st.title("Sistema de Validação e Geração de Leads para CRM")

uploaded_file = st.file_uploader("Importe a planilha bruta (formato Excel)", type=["xlsx"])

with st.sidebar:
    sindicato = st.text_input("Sindicato")
    categoria = st.text_input("Categoria")
    beneficio = st.selectbox("Benefícios", list(CAMPO_BENEFICIOS_NEGOCIACAO.keys()))
    beneficios_cct = st.multiselect("Benefícios previstos em CCT", list(CAMPO_BENEFICIOS_CCT.keys()))
    cadencia = st.text_input("Cadência Meetime")

if uploaded_file:
    df = pd.read_excel(uploaded_file)
    resultado = tratar_planilha(df, sindicato, categoria, beneficio, cadencia, beneficios_cct)
    st.success("Validação concluída com sucesso!")

    colunas_resultado = [
        "Proprietario", "Pessoa", "Benefícios   em negociação", "Razão",
        "CNPJ", "CNPJ (CE)", "Email", "Telefone 1", "Telefone 2", "Sindicato",
        "Categoria", "Endereço", "Número", "Complemento", "Bairro", "Cidade",
        "CEP", "UF", "Benefícios previstos em CCT",
        "Quadro de Funcionários", "Cadência Meetime"
    ]
    colunas_resultado = list(dict.fromkeys([c for c in colunas_resultado if c in resultado.columns]))
    st.dataframe(resultado[colunas_resultado])

    leads_nao_cadastrados = resultado[resultado['Empresa Cadastrada'].str.upper() == 'NÃO']

    if st.button("Importar no CRM"):
        with st.spinner("Importando leads para o CRM..."):
            mensagens = importar_no_crm(leads_nao_cadastrados)
            for msg in mensagens:
                st.write(msg)

    excel_data = exportar_excel(resultado)
    st.download_button("Exportar planilha tratada", data=excel_data, file_name="resultado_validado.xlsx")
