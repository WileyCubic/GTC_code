# GTC Code Repository

Central codebase for **Game Time Couture** business operations, analytics, and data processing.

## 🏗️ Project Structure

- **`Data-Pipelines/`** - ETL pipelines for Square/Shopify sales data processing
- **`Analysis-Bashboard/`** - Interactive dashboards and business analytics
- **`gtc-graphql-api/`** - Embedded Shopify app (React Router + Prisma)
- **`shopify-graphql-retrieve-orders/`** - Shopify order data retrieval via GraphQL API (Does not work currently)
- **`GTC-Formated-Privot-Tables/`** - Formatted reporting and pivot tables
- **`scripts/`** - Automation and utility scripts

## ⚙️ Setup

All projects in this repo share a single root `.env` file.

1. Copy the template: `cp .env.example .env`
2. Fill in real values in `.env` (never commit this file).
3. Install Python dependencies (editable install, covers every Python project): `pip install -e .` (add `[test]` for the test suite: `pip install -e ".[test]"`)
4. For the Node projects, install dependencies per-project (`cd gtc-graphql-api/src && npm install`, `cd shopify-graphql-retrieve-orders/src && npm install`) — they read from the same root `.env` automatically.

## 🚀 Key Features

- **Data Pipeline**: Pipelines for sales and expense data ETL/ELT
- **Real-time Analytics**: Sales performance and customer insights  
- **Automated Reporting**: Scheduled data processing and reports
- **GraphQL APIs**: Flexible data access and integration
- **Interactive Dashboards**: Business intelligence and visualization

## 🛠️ Tech Stack

- **Python**: Data processing, ETL pipelines
- **SQL**: Database operations (MySQL, SQLite)
- **GraphQL**: API services
- **Pandas**: Data analysis and transformation
- **Dash/Plotly**: Interactive dashboards



## 📜 License

This project is licensed under the **MIT License** – see the [LICENSE](LICENSE) file for details.

---

## 🌐 **Company:** Game Time Couture / GTC Greek
<p align='center'>
    <img src=https://8d6f392e60e520ff48d4.cdn6.editmysite.com/uploads/b/8d6f392e60e520ff48d435335921b21eff5fef11c4c62075ccbb7fc0b9906752/8.8.2024_66b4e208d312b8.66067049.png?width=2400&optimize=medium alt='Game Time Couture' width='200'>
    <img src=https://cdn.shopify.com/s/files/1/0710/3145/5940/files/GTC_LOGO-02.png?v=1749586626 alt='Game Time Couture' width='500'>
</p>
