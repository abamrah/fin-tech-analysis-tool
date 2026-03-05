"""
Flashcard content library — pre-built decks covering core financial literacy.
Each deck contains 8-12 cards. Seeded on first access.
"""

DECKS = [
    {
        "slug": "banking-basics",
        "title": "Banking Basics",
        "description": "Essential knowledge about bank accounts, fees, and services.",
        "icon": "🏦",
        "category": "banking",
        "difficulty": "beginner",
        "cards": [
            {
                "front": "What is the difference between a chequing and a savings account?",
                "back": "A chequing account is for daily transactions (debit card, bill payments). A savings account earns interest but may limit withdrawals. Keep your emergency fund in savings, daily spending money in chequing.",
                "hint": "Think about frequency of access",
            },
            {
                "front": "What is an NSF fee?",
                "back": "Non-Sufficient Funds fee — charged when you try to make a payment but don't have enough money in your account. Typically $45-48 in Canada. Set up low-balance alerts to avoid this.",
                "hint": "Happens when you overdraw",
            },
            {
                "front": "What is a GIC?",
                "back": "Guaranteed Investment Certificate — you lock your money for a fixed term (1-5 years) and earn guaranteed interest. Higher rates than savings accounts, but your money is locked until maturity.",
                "hint": "A guaranteed return investment",
            },
            {
                "front": "What does CDIC insurance cover?",
                "back": "Canada Deposit Insurance Corporation protects eligible deposits (savings, chequing, GICs under 5 years) up to $100,000 per category per member institution if a bank fails.",
                "hint": "Protection if your bank fails",
            },
            {
                "front": "What is direct deposit and why should you use it?",
                "back": "Direct deposit automatically sends your pay into your bank account. Benefits: faster access to money (no cheque hold), no risk of lost cheques, and easier to automate savings.",
            },
            {
                "front": "What is a pre-authorized debit (PAD)?",
                "back": "An agreement allowing a company to withdraw money from your account on a schedule (e.g., rent, subscriptions). Always track these — you can cancel within 90 days if unauthorized.",
            },
            {
                "front": "What is an e-Transfer and are there fees?",
                "back": "Interac e-Transfer sends money between Canadian bank accounts via email/phone. Most major banks include free e-Transfers with their accounts. There may be daily/weekly limits ($3,000-$10,000 typically).",
            },
            {
                "front": "What's the difference between a bank and a credit union?",
                "back": "Banks are for-profit corporations. Credit unions are member-owned cooperatives — often lower fees, better savings rates, but fewer branches. Both have deposit insurance (CDIC vs provincial).",
            },
        ],
    },
    {
        "slug": "budgeting-101",
        "title": "Budgeting 101",
        "description": "Master the 50/30/20 rule and practical budgeting strategies.",
        "icon": "💰",
        "category": "budgeting",
        "difficulty": "beginner",
        "cards": [
            {
                "front": "What is the 50/30/20 rule?",
                "back": "A budgeting guideline: 50% of after-tax income goes to Needs (rent, groceries, insurance), 30% to Wants (dining, entertainment), and 20% to Savings/Debt repayment. It's a starting point — adjust to your situation.",
                "hint": "Three percentage buckets",
            },
            {
                "front": "What's the difference between fixed and variable expenses?",
                "back": "Fixed expenses stay the same each month (rent, insurance, subscriptions). Variable expenses fluctuate (groceries, gas, dining). Track variable expenses closely — that's where budget leaks happen.",
            },
            {
                "front": "What is 'pay yourself first'?",
                "back": "Automatically transfer money to savings as soon as you get paid — before spending on anything else. Treat savings like a non-negotiable bill. Even $50/paycheque adds up to $1,300/year.",
                "hint": "Savings as a priority, not leftovers",
            },
            {
                "front": "What is a sinking fund?",
                "back": "Money set aside monthly for a known future expense (car maintenance, holiday gifts, annual insurance). Example: $100/month for 6 months = $600 ready for December gifts without going into debt.",
            },
            {
                "front": "What is lifestyle inflation?",
                "back": "When your spending increases as your income rises — you earn more but save the same (or less). Combat it by banking at least 50% of every raise into savings before adjusting your lifestyle.",
            },
            {
                "front": "What is zero-based budgeting?",
                "back": "Every dollar of income is assigned a job (expenses, savings, debt, fun) so income minus planned spending equals zero. This doesn't mean spending everything — savings is a 'job' too.",
            },
            {
                "front": "How often should you review your budget?",
                "back": "Weekly: quick check on spending pace. Monthly: full review and adjust limits. Quarterly: bigger picture — are you on track for annual goals? Use the Weekly Tune feature to auto-adjust!",
            },
            {
                "front": "What is the envelope method?",
                "back": "Allocate cash into physical (or digital) envelopes for each spending category. When an envelope is empty, stop spending in that category. Forces awareness and prevents overspending.",
            },
        ],
    },
    {
        "slug": "credit-mastery",
        "title": "Credit Mastery",
        "description": "Understand credit scores, credit cards, and responsible borrowing.",
        "icon": "💳",
        "category": "credit",
        "difficulty": "intermediate",
        "cards": [
            {
                "front": "What factors make up your credit score?",
                "back": "Payment history (35%) — pay on time! Credit utilization (30%) — keep below 30% of limit. Length of history (15%). Credit mix (10%). New inquiries (10%). Focus on the top two for biggest impact.",
                "hint": "Five factors, two matter most",
            },
            {
                "front": "What is credit utilization and what's ideal?",
                "back": "The percentage of your available credit you're using. If your limit is $5,000 and balance is $1,500, utilization is 30%. Ideal: under 30%, excellent: under 10%. Pay before statement date to lower it.",
            },
            {
                "front": "What is the minimum payment trap?",
                "back": "Paying only the minimum (usually 2-3% of balance) means most goes to interest. A $5,000 balance at 20% APR with minimum payments takes 30+ years to pay off and costs $12,000+ in interest. Always pay more than minimum.",
            },
            {
                "front": "What is a hard vs soft credit inquiry?",
                "back": "Hard inquiry: when you apply for credit (shows on report, slight score dip). Soft inquiry: checking your own score or pre-approvals (no impact). Multiple hard inquiries for the same type within 14-45 days count as one.",
            },
            {
                "front": "Should you close old credit cards?",
                "back": "Generally no — closing cards reduces your total available credit (raises utilization) and shortens credit history. If a no-fee card, keep it open with a small recurring charge. Close only if it has an annual fee you can't justify.",
            },
            {
                "front": "What is the grace period on a credit card?",
                "back": "The 21-25 day window after your statement date when you can pay in full with zero interest. Key: this only applies if you paid last month's balance in full. Carrying a balance means interest starts immediately on new purchases.",
            },
            {
                "front": "How do balance transfers work?",
                "back": "Move high-interest debt to a card offering 0% promotional APR (usually 6-12 months). Typically 1-3% transfer fee. Strategy: transfer, set up auto-payments to pay it off before promo ends. Don't add new charges!",
            },
            {
                "front": "What goes on a credit report?",
                "back": "Payment history, account balances, credit limits, account age, public records (bankruptcies), and inquiries. Does NOT include: income, savings, debit card usage, or rent (unless landlord reports it).",
            },
            {
                "front": "What is a good credit score in Canada?",
                "back": "Ranges from 300-900. Below 560: poor. 560-659: fair. 660-724: good. 725-759: very good. 760+: excellent. Most lenders want 650+ for approval, 750+ for best rates.",
            },
        ],
    },
    {
        "slug": "investing-fundamentals",
        "title": "Investing Fundamentals",
        "description": "Key concepts for growing your wealth through investing.",
        "icon": "📈",
        "category": "investing",
        "difficulty": "intermediate",
        "cards": [
            {
                "front": "What is compound interest?",
                "back": "Earning interest on your interest. $10,000 at 7% annual return: Year 1 = $10,700, Year 10 = $19,672, Year 30 = $76,123. Time is the most powerful factor — start early, even with small amounts.",
                "hint": "Interest on interest",
            },
            {
                "front": "What is the difference between a TFSA and RRSP?",
                "back": "TFSA: contribute after-tax money, grow and withdraw tax-free. Best if you expect higher income later. RRSP: contribute pre-tax (get deduction now), pay tax on withdrawal. Best if you're in a higher tax bracket now than in retirement.",
            },
            {
                "front": "What is an ETF?",
                "back": "Exchange-Traded Fund — a basket of stocks/bonds that trades on an exchange like a single stock. Low fees (0.03-0.5% MER), instant diversification. Example: VFV tracks the S&P 500 for ~0.08% MER.",
            },
            {
                "front": "What is dollar-cost averaging?",
                "back": "Investing a fixed amount on a regular schedule regardless of market conditions. When prices are low, you buy more units. When high, fewer. This removes emotion and timing risk from investing.",
            },
            {
                "front": "What is the difference between stocks and bonds?",
                "back": "Stocks: partial ownership in a company (higher risk, higher potential return). Bonds: you lend money to a government/company (lower risk, fixed interest). A balanced portfolio holds both — ratio depends on your timeline and risk tolerance.",
            },
            {
                "front": "What is MER and why does it matter?",
                "back": "Management Expense Ratio — the annual fee charged by a fund. A 2% MER on $100,000 costs $2,000/year. Over 30 years, a 2% vs 0.2% MER difference can cost you $200,000+ in lost returns. Always check MER before investing!",
            },
            {
                "front": "What is asset allocation?",
                "back": "How you divide investments between stocks, bonds, and cash. Common rule of thumb: 100 minus your age = stock %. A 30-year-old might hold 70% stocks, 25% bonds, 5% cash. Rebalance annually.",
            },
            {
                "front": "What is the FHSA?",
                "back": "First Home Savings Account — new Canadian account combining TFSA + RRSP benefits. Contribute up to $8,000/year ($40,000 lifetime), get a tax deduction like RRSP, and withdraw tax-free for your first home. Best of both worlds!",
            },
            {
                "front": "What is diversification?",
                "back": "Spreading investments across different asset types, sectors, and geographies to reduce risk. If one investment drops, others may hold steady or rise. 'Don't put all your eggs in one basket' — but 1-2 broad ETFs can achieve this.",
            },
        ],
    },
    {
        "slug": "tax-essentials",
        "title": "Canadian Tax Essentials",
        "description": "Deductions, credits, and strategies to keep more of your money.",
        "icon": "🧾",
        "category": "taxes",
        "difficulty": "intermediate",
        "cards": [
            {
                "front": "What's the difference between a tax deduction and a tax credit?",
                "back": "Deduction: reduces your taxable income (e.g., RRSP contribution of $5,000 removes $5,000 from taxable income). Credit: directly reduces tax owed (e.g., $300 credit = $300 less tax). Credits are generally more valuable dollar-for-dollar.",
                "hint": "One reduces income, one reduces tax",
            },
            {
                "front": "What is the RRSP contribution deadline?",
                "back": "60 days after December 31 (usually March 1, or February 29 in leap years). Contributions within this window can be deducted on the previous year's tax return. Max: 18% of previous year's earned income, up to the annual limit.",
            },
            {
                "front": "What are the 2025 federal tax brackets?",
                "back": "15% on first ~$57,375. 20.5% on $57,375-$114,750. 26% on $114,750-$158,468. 29% on $158,468-$220,000. 33% above $220,000. Provincial tax is added on top. Remember: only income IN each bracket is taxed at that rate.",
            },
            {
                "front": "What is the basic personal amount?",
                "back": "The amount of income you can earn tax-free — approximately $15,705 (2025). This means the first ~$15,705 of income has zero federal tax. Claimed automatically on your tax return.",
            },
            {
                "front": "Can you claim work-from-home expenses?",
                "back": "Yes — two methods: Flat rate ($2/day up to $500) or Detailed method (requires T2200 from employer, track actual expenses: utilities, internet, rent proportional to office space).",
            },
            {
                "front": "What is income splitting and who can do it?",
                "back": "Shifting income to a lower-income spouse/partner to reduce overall family tax. Methods: spousal RRSP, pension income splitting (after 65), prescribed rate loans. Not all splitting strategies are legal — consult an accountant.",
            },
            {
                "front": "What happens if you over-contribute to your TFSA?",
                "back": "1% penalty tax per month on the excess amount until you withdraw it. Track your contribution room on CRA My Account. Contribution room carries forward from age 18 — check your lifetime total before contributing.",
            },
            {
                "front": "What is a capital gain and how is it taxed?",
                "back": "Profit from selling an investment for more than you paid. In Canada, 50% of capital gains are added to taxable income (the 'inclusion rate' — increased to 66.7% above $250K in 2024). Gains inside TFSA/RRSP are sheltered.",
            },
        ],
    },
    {
        "slug": "saving-strategies",
        "title": "Saving Strategies",
        "description": "Practical techniques to build your savings faster.",
        "icon": "🐷",
        "category": "saving",
        "difficulty": "beginner",
        "cards": [
            {
                "front": "How much should you have in an emergency fund?",
                "back": "3-6 months of essential expenses (rent, groceries, insurance, utilities). If self-employed or single income: aim for 6-9 months. Keep it in a high-interest savings account — accessible but separate from daily spending.",
                "hint": "Months of essential expenses",
            },
            {
                "front": "What is the 24-hour rule?",
                "back": "Before making any non-essential purchase over a set amount ($50-100), wait 24 hours. This cooling period prevents impulse buys — studies show 50-70% of the time you won't go back to buy it.",
            },
            {
                "front": "What is the latte factor?",
                "back": "Small daily expenses that add up dramatically. $5/day coffee = $1,825/year. $15/day (coffee + lunch + snacks) = $5,475/year. Invested at 7% for 30 years, that's $500,000+. Small changes, huge long-term impact.",
            },
            {
                "front": "What is a high-interest savings account (HISA)?",
                "back": "A savings account earning 3-5%+ interest (vs 0.01-0.5% at big banks). Available at online banks (EQ Bank, Tangerine, etc.). CDIC insured, no risk. Perfect for emergency fund and short-term savings goals.",
            },
            {
                "front": "What is the 'round-up' savings method?",
                "back": "Round every purchase up to the nearest dollar and save the difference. Bought coffee for $4.75? Round to $5, save $0.25. Many banks offer this automatically. Painless way to save $300-600/year.",
            },
            {
                "front": "Should you save or pay off debt first?",
                "back": "Priority order: 1) Small emergency fund ($1,000-2,000). 2) Pay off high-interest debt (credit cards > 15%). 3) Build full emergency fund. 4) Invest. Exception: always get employer RRSP match — it's free money.",
            },
            {
                "front": "What is the 'no-spend challenge'?",
                "back": "Commit to spending only on essentials for a set period (1 day/week/month). Rules: no dining out, no shopping, no entertainment purchases. Eye-opening for seeing how much discretionary spending you do unconsciously.",
            },
            {
                "front": "How can you negotiate bills to save money?",
                "back": "Call your provider annually for: internet, phone, insurance. Say 'I'm considering switching — what can you offer?' Average savings: $20-50/month per service. That's $240-600/year for a few phone calls.",
            },
        ],
    },
    {
        "slug": "debt-management",
        "title": "Debt Management",
        "description": "Strategies to eliminate debt efficiently and stay debt-free.",
        "icon": "⚖️",
        "category": "credit",
        "difficulty": "intermediate",
        "cards": [
            {
                "front": "What is the avalanche method of debt repayment?",
                "back": "Pay minimums on all debts, then throw extra money at the highest-interest debt first. Mathematically optimal — saves the most in interest. Best for disciplined savers.",
                "hint": "Highest interest first",
            },
            {
                "front": "What is the snowball method of debt repayment?",
                "back": "Pay minimums on all debts, put extra money toward the smallest balance first. Less optimal mathematically, but the quick wins create momentum and motivation. Best if you need psychological boosts.",
                "hint": "Smallest balance first",
            },
            {
                "front": "What is a debt-to-income ratio?",
                "back": "Total monthly debt payments ÷ gross monthly income × 100. Example: $1,500 debt payments on $5,000 income = 30%. Under 36%: healthy. 36-49%: caution. 50%+: danger zone. Lenders use this to assess creditworthiness.",
            },
            {
                "front": "What is good debt vs bad debt?",
                "back": "Good debt: low-interest, builds wealth or income (mortgage, student loans, business loans). Bad debt: high-interest, depreciating assets (credit cards, car loans for luxury vehicles, payday loans). Minimize bad debt aggressively.",
            },
            {
                "front": "What are payday loans and why avoid them?",
                "back": "Short-term loans at extreme interest rates — $15-25 per $100 borrowed for 2 weeks = 390-650% APR. Creates a debt cycle. Alternatives: bank overdraft, credit card cash advance (still bad but cheaper), community loans.",
            },
            {
                "front": "What is a consumer proposal?",
                "back": "A legal debt settlement in Canada — you offer creditors a portion of what you owe (often 30-70 cents on the dollar), paid over up to 5 years. Less severe than bankruptcy. Stays on credit report for 3 years after completion.",
            },
            {
                "front": "How does mortgage amortization work?",
                "back": "Standard Canadian mortgage: 25-year amortization, 5-year term. Early payments are mostly interest. Making even $100/month extra payment or switching to bi-weekly can save tens of thousands and years off the mortgage.",
            },
            {
                "front": "Should you consolidate your debts?",
                "back": "If you can get a lower rate than your current average: yes. A consolidation loan at 8% to pay off 20%+ credit cards saves money. Warning: don't run up the credit cards again after consolidating — that's a common trap.",
            },
        ],
    },
]
