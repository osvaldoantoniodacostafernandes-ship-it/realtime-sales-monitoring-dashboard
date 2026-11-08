# GIA KOMPLETU APREZENTASAUN POWERPOINT (Lian Tetun)
## Capstone Project Big Data – Topik #11: Real-Time Sales Dashboard
**Instituisaun:** Dili Institute of Technology (DIT)  
**Fakuldade:** Siencia Komputador | Tinan Akadémiku 2026  
**Estudu Kazu:** Toko Loilary Supermarket – Dili, Timor-Leste  

---

## Estrutura Slide 1 to 13 (Konteúdu Slide & Testu Lian Aprezentasaun)

---

### SLIDE 1: Títulu & Identidade Projetu
* **Títulu Slide:** REAL-TIME SALES MONITORING DASHBOARD
* **Sub-títulu:** Sistema Monitorizasaun Venda no Rendimentu Tempu Real bazeia ba Big Data Pipeline
* **Konteúdu Slide:**
  * **Topik #11:** Real-Time Sales Dashboard
  * **Estudu Kazu:** Toko Loilary Supermarket – Dili, Timor-Leste
  * **Estudante / Grupu:** [Prenxe Ita-Boot nia Naran & No. Estudante]
  * **Orientadór / Dosente:** [Prenxe Naran Dosente]
  * **Departamentu:** Siencia Komputador, Dili Institute of Technology (DIT) | 2026
* **Lian/Diskursu Aprezentasaun (Oinsá atu ko'alia):**
  > *"Bondia / Botarde ba dosente orientadór no dosente examinadór sira hotu. Obrigadu ba oportunidade ne'ebé fó mai ha'u/ami. Ohin ha'u/ami sei aprezenta rezultadu Capstone Project ba unidade kurikuler Big Data ho títulu: **Real-Time Sales Dashboard**, ho estudu kazu iha Toko Loilary Supermarket, Dili. Projetu ne'e foka liu ba oinsá integra pipeline Big Data husi rekolha dadus to'o visualizasaun dashboard iha tempu real."*

---

### SLIDE 2: Kontestu & Introdusaun
* **Títulu Slide:** Kontestu & Introdusaun (Background)
* **Konteúdu Slide (Ponto Prinsipál):**
  * Toko Loilary Supermarket mak negosiu retallu boot ida iha Dili ne'ebé atende tranzasaun kaixa barak loron-loron.
  * Presiza jestaun finanseira ne'ebé lalais, la bele hein to'o kalan foin hatene total rendimentu.
  * Tranzasaun hotu uza moeda USD ($) ho métodu pagamentu **100% Cash**.
  * Difikuldade jestór atu deteta kedas tranzasaun ne'ebé anómala ka lakon sasán.
* **Lian/Diskursu Aprezentasaun:**
  > *"Toko Loilary iha Dili halo operasaun fa'an sasán hanesan ai-han, hemu, no nesesidade bázika loron-loron. Karaterístika negosiu ne'e mak tranzasaun kaixa la'o beibeik ho métodu 100% Cash. Problema baibain mak jestór susar atu hatene omzet iha tempu ne'e kedas se la halo kalkulasaun manuál iha kalan. Tan ne'e, presiza tebes sistema Big Data ne'ebé bele fó informasaun instantáneu."*

---

### SLIDE 3: Formulasaun Problema (Problem Statement)
* **Títulu Slide:** Formulasaun Problema (Problem Statement)
* **Konteúdu Slide (Ponto Prinsipál):**
  1. **Late Processing (Atrasu Dadus):** Relatóriu venda tradisionál tarde liu (hein to'o operasaun taka).
  2. **Lack of Live Monitoring (Laiha Monitorizasaun Direta):** Susar atu hatene produtu ida-ne'ebé mak fa'an lalais liu iha oras ne'e kedas.
  3. **Fraud & Anomaly Risk (Risku Fraude & Anomalia):** Laiha alerta automátiku bainhira iha tranzasaun ne'ebé folin la normál ka kuanitade boot liu.
  4. **No Predictive Insights (Laiha Previsaun Futuru):** Jestaun stok no rendimentu la bazeia ba dadus preditivu (Machine Learning).
* **Lian/Diskursu Aprezentasaun:**
  > *"Problema prinsipál ne'ebé ami identifika iha terrenu mak: Primeiru, atrasu relatóriu venda tanba hein prosesu manuál. Segundu, laiha monitorizasaun ba kategoria produtu ne'ebé fa'an di'ak iha oras ponta (peak hours). Terseiru, susar kontrola anomalia ka erru kaixa. No ikus liu, la iha mekanizmu previsaun ba rendimentu horas oin mai hodi ajuda desizaun negósiu."*

---

### SLIDE 4: Objetivu Projetu (Project Objectives)
* **Títulu Slide:** Objetivu Projetu (Project Objectives)
* **Konteúdu Slide (Ponto Prinsipál):**
  * Dezenvolve **Real-Time Data Pipeline** hodi halao ingestao dadus kaixa kontinua uza **Apache Kafka**.
  * Aplika **Apache Spark** ba prosesamentu distribuídu (Streaming & Batch Processing) inklui Feature Engineering (lukru, margem, peak hour).
  * Implementa **Machine Learning** ba previsaun rendimentu (Regression) no detesaun tranzasaun suspeita (Z-Score & IQR).
  * Kria **Interactive Dashboards** uza **Microsoft Power BI** no **Web Dashboard (Flask)** ho 100% lian Inglés.
  * Kumpri rekezitu tékniku mínimu kuríkulu DIT 2026 ($\ge$ 10.000 records).
* **Lian/Diskursu Aprezentasaun:**
  > *"Objetivu prinsipál husi projetu ne'e mak harii arkitetura Big Data kompletu husi ponta ba ponta. Husi Ingestion uza Kafka, prosesamentu distribuídu uza Spark, armazenamentu iha MySQL, to'o analiza Machine Learning no visualizasaun iha Power BI no Web Dashboard, hodi rezolve problema jestaun iha Toko Loilary."*

---

### SLIDE 5: Implementasaun 5V Big Data iha Projetu
* **Títulu Slide:** Konseitu 5V Big Data (Karakterístika Sistema)
* **Konteúdu Slide:**
  * **Volume:** Jestaun tranzasaun riil **15.000 records** iha MySQL, no teste to'o **1.000.000 records** (lulus uji skalabilidade).
  * **Velocity:** Aliran dadus la'o kontinua kada **0.5 – 2 segundu** liuhusi Apache Kafka topic.
  * **Variety:** Dadus **Semi-Structured JSON** husi kaixa transforma ba **Relational Structured Data** iha MySQL.
  * **Veracity:** Garantia kualidade dadus: **0 missing values**, **0 duplikasaun**, no validasaun outlier.
  * **Value:** Fó valór estratéjiku: rendimentu totál ($335.09K), lukru estimadu ($101.04K), no previsaun 6 oras ba oin.
* **Lian/Diskursu Aprezentasaun:**
  > *"Projetu ne'e kumpri konseitu 5V Big Data: Volume ho 15.000 tranzasaun riil no kbiit to'o 1 millaun dadus; Velocity liuhusi fluxu Kafka kada 0.5 to'o 2 segundu; Variety hodi simu JSON semi-structured no konverte ba SQL; Veracity tanba dadus 100% moos laiha null; no Value tanba fó benefísiu finanseiru riil ba empreza."*

---

### SLIDE 6: Fonte Dadus & Esplikasaun 209 Produtu Terrenu
* **Títulu Slide:** Fonte Dadus & Metodolojia (Terrenu vs Simulasaun)
* **Konteúdu Slide:**
  * **Master Catalog Lapangan (209 Produtu Riil):**
    * Ami foti dadus fíziku diretamente husi prateleira Toko Loilary Supermarket Dili.
    * Kobre kategoria 10: *Beverages, Food & Snacks, Groceries, Dairy, Sauces, Household, etc.*
    * Folin ofisiál iha moeda USD ($).
  * **Simulated Real-Time POS Cashier Streaming:**
    * Bazeia ba **Manual Capstone Bab 2 & Bab 5.1** ne'ebé autoriza *"Simulated Real-Time Data"*.
    * Python Producer foti aleatoriamente husi produtu 209 riil ne'e hodi simula kompras kliente loron-loron (Qty 1–5, 100% Cash).
    * Rezultadu: **15.000 tranzasaun riil** ho padraun belanja normál.
* **Lian/Diskursu Aprezentasaun (Ponto Importante ba Dosente!):**
  > *"Dosente sira, konaba dadus 209 ne'ebé ami rekolha husi terrenu, dadus ne'e reprezenta **Master Catalog** produtu loja nian ho folin riil merkadu Dili. Tanba manual capstone iha Ponto 2 no Ponto 5.1 autoriza uza **Simulated Real-Time Data**, ami dezenvolve produser hodi halo simulasaun kaixa ne'ebé kontinua foti husi lista produtu 209 ne'e. Nune'e ami bele kria tranzasaun hamutuk 15.000 ho métodu 100% Cash hodi atinji padraun Big Data $\ge$ 10.000 dadus."*

---

### SLIDE 7: Arkitetura Big Data Pipeline (System Architecture)
* **Títulu Slide:** Diagrama Arkitetura Sistema (Big Data Pipeline)
* **Konteúdu Slide:**
  *(Tudu diagrama banner ne'ebé iha `docs/bigdata_pipeline_banner.jpg`)*
  * **1. Data Source:** POS Toko Loilary (209 Master Catalog, JSON Format)
  * **2. Data Ingestion:** Apache Kafka Broker 7.5.0 (Topic: `sales_transactions` @ port 9092)
  * **3. Data Processing:** Apache Spark 3.5.0 (Streaming Cleaning, Join, Features & Batch)
  * **4. Storage:** MySQL 8.0 (`sales_dashboard` ho tabela 10)
  * **5. Analytics & ML:** OLS Forecast, Z-Score Anomaly Alerts, K-Means Clustering
  * **6. Visualization:** Microsoft Power BI & Web Dashboard (Flask / Streamlit)
* **Lian/Diskursu Aprezentasaun:**
  > *"Iha slide ne'e ami hatudu arquitetura kompletu husi pipeline Big Data. Dadus komesa husi Kaixa POS, tama ba Apache Kafka nu'udar broker ingestion, tuirmai Apache Spark halo ingestao micro-batch, hamoos dadus, no halo JOIN ho tabela katalog produtu. Rezultadu ne'e rai iha MySQL 8.0, analiza liuhusi algoritmu Machine Learning, no aprezenta iha Microsoft Power BI no Web Dashboard."*

---

### SLIDE 8: Prosesamentu Distribuídu (Apache Spark)
* **Títulu Slide:** Stream & Batch Processing (Apache Spark 3.5.0)
* **Konteúdu Slide:**
  * **Broadcast JOIN:** Halo ligasaun direta entre aliran tranzasaun ho metadata produtu (`supplier`, `margin_rate`).
  * **Feature Engineering:**
    * `estimated_profit = total_amount * margin_rate`
    * `is_high_value = tranzasaun >= $35.00`
    * `is_peak_hour = oras 11-13 no 17-19`
  * **Spark Batch Analytics (LO5):**
    * Agregasaun harian: `daily_sales_batch` (rendimentu diáriu & oras peak hour).
    * Ranking produtu: `top_products_batch` uza window function `dense_rank()`.
* **Lian/Diskursu Aprezentasaun:**
  > *"Iha faze prosesamentu, Apache Spark la'os de'it simu dadus, maibé halo Feature Engineering automátiku: kalkula lukru direta, fó marka ba kompras boot (high value), no deteta oras movimentu boot (peak hours). Spark mós halao prosesu Batch hodi kalkula total venda diáriu no fó ranking ba produtu 10 ne'ebé fa'an maka'as liu iha Timor-Leste."*

---

### SLIDE 9: Big Data Analytics & Machine Learning (Bab 5.5)
* **Títulu Slide:** Machine Learning & Modelu Estatístika (10% Avaliasaun)
* **Konteúdu Slide:**
  1. **Sales & Revenue Forecasting (Regression):**
     * Modelu: Multivariate Time-Series OLS Regression ($R^2 = 0.61$).
     * Prevee rendimentu 6 oras ba oin kompletu ho intervalu konfiansa 95% (Upper & Lower Bound).
  2. **Real-Time Anomaly Detection (Outlier Alerts):**
     * Métodu: Statistical Z-Score no Interquartile Range (IQR).
     * Deteta automatikamente tranzasaun ne'ebé la normál (kuantidade boot ka folin aas liu).
  3. **Product Segmentation (K-Means Clustering):**
     * Fahe produtu ba klúster 3: *Top Contributor (High Revenue)*, *Fast-Moving Consumer Goods*, no *Standard Steady Goods*.
* **Lian/Diskursu Aprezentasaun:**
  > *"Tuir rekezitu Bab 5.5, ami implementa téknika Machine Learning tolu: Primeiru, Regresaun multivariada hodi prevee omzet 6 oras ba oin; Segundu, detesaun anomalia uza Z-Score no IQR hodi fó alerta kedas se iha tranzasaun suspeita; Terseiru, K-Means Clustering hodi fahe produtu ba grupu estratéjiku tolu bazeia ba volume no rendimentu."*

---

### SLIDE 10: Visualizasaun & Dashboard (Power BI & Web)
* **Títulu Slide:** Visualizasaun Dashboard (Power BI & Web Flask)
* **Konteúdu Slide:**
  * **Tampilan 100% Lian Inglés:**
    * **4 Kartu KPI:** Revenue ($335.09K), Total Transactions (15K), Total Units (45K), Avg. Order Value ($22.34).
    * **Sales by Product Category:** Gráfiku barra (Alcohol, Beer, Food, Beverages, etc.).
    * **Payment Method:** Donut chart hatudu **100% Cash** ($335.09K).
    * **ML Forecast Chart:** Liña previsaun rendimentu & Upper Bound.
    * **Top 5 Product:** Gráfiku barra orizontál produtu terlaris.
    * **Live Cashier Transactions:** Tabela 12 tranzasaun ikus ho auto-update kada segundu 3.
  * **Microsoft Power BI:** DirectQuery mode, Star Schema, no 10 formula formál DAX Measures.
* **Lian/Diskursu Aprezentasaun:**
  > *"Iha parte visualizasaun, ami dezenvolve dashboard rua: Web Dashboard uza Flask ho auto-refresh kada segundu 3, no Microsoft Power BI ne'ebé liga direta liuhusi DirectQuery. Dashboard rua ne'e uza 100% lian Inglés, hatudu KPI omzet, pangsa 100% Cash, gráfiku kategoria, previsaun ML, Top 5 Produtu, no tabela tranzasaun live husi kaixa Toko Loilary."*

---

### SLIDE 11: Rezultadu Teste no Avaliasaun (Bab 14)
* **Títulu Slide:** Rezultadu Teste no Avaliasaun (Testing & Evaluation)
* **Konteúdu Slide:**
  * **A. Functional Testing (Bab 14.A):** 100% PASS (Kafka, 10 Tabela MySQL, Spark Features, Dashboard API).
  * **B. Data Quality Testing (Bab 14.C):** 100% PASS (0 null values, 0 duplikasaun, kalkulasaun konsistente).
  * **C. Performance Testing (Bab 14.B):** Throughput **124.374 records/segundu**, laténsia média **7.74 ms**, CPU 26%.
  * **D. Scalability Testing (Bab 14.D):**
    * 10.000 records $\rightarrow$ 0.031 segundu (322k rec/s)
    * 100.000 records $\rightarrow$ 0.083 segundu (1.19M rec/s)
    * **1.000.000 records $\rightarrow$ 0.758 segundu (1.31M rec/s)**
    * *Konkluzaun: Skalabilidade lineár $O(N)$ to'o 1 millaun dadus.*
* **Lian/Diskursu Aprezentasaun:**
  > *"Ami halao teste formál haat tuir Bab 14 manual nian: Functional Testing 100% liu; Data Quality moos 100% laiha dadus mamuk; Performance teste hatudu velosidade 124.000 dadus kada segundu ho laténsia ki'ik 7.7 ms; no Scalability teste prova katak sistema bele prosesa to'o dadus millaun 1 iha tempu menus husi segundu 1."*

---

### SLIDE 12: Konkluzaun & Dezenvolvimentu Futuru
* **Títulu Slide:** Konkluzaun & Dezenvolvimentu Futuru (Conclusion)
* **Konteúdu Slide:**
  * **Konkluzaun:**
    * Projetu Capstone Big Data ba Toko Loilary Supermarket **100% susesu implementadu**.
    * Pipeline kobre hotu rekezitu kuríkulu: Ingestion, Processing, Analytics, no Dashboard.
    * Solusaun ne'e rezolve duni atrasu relatóriu diáriu no hadi'a jestaun finanseira loja nian.
  * **Dezenvolvimentu Futuru (Future Work):**
    * Implementa deployment ba Cloud (AWS / Google Cloud / Kubernetes).
    * Integra direta ho mákina POS fiziku liuhusi IoT barcode scanner.
* **Lian/Diskursu Aprezentasaun:**
  > *"Nu'udar konkluzaun, sistema ne'e prova katak teknolojia Big Data bele aplika ho susesu ba negosiu retallu lokal iha Timor-Leste. Sistema ne'e ajuda jestór Toko Loilary atu halo desizaun bazeia ba dadus tempu real, la'os de'it sentimentu. Ba futuru, sistema ne'e bele haboot liután hodi instala ba cloud no liga diretamente ba mákina barcode scanner iha loja."*

---

### SLIDE 13: Sesaun Kestionáriu & Agradesimentu
* **Títulu Slide:** Kestionáriu & Obrigadu Barak (Q & A)
* **Konteúdu Slide:**
  * **Obrigadu Barak ba Atensaun!**
  * *Dili Institute of Technology (DIT) | S1 Siencia Komputador | 2026*
  * Prontu simu pergunta no sujestaun husi Dosente Examinadór sira.
* **Lian/Diskursu Aprezentasaun:**
  > *"Ne'e mak aprezentasaun husi ha'u/ami konaba projetu Big Data Real-Time Sales Dashboard Toko Loilary. Ha'u/ami prontu atu demonstra sistema live no simu pergunta husi dosente sira. Obrigadu barak."*
