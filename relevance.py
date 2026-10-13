"""Retail-facing event classification and event-to-security relevance rules."""
import re

THEME_PATTERNS = [
    ("taiwan", r"\btaiwan\b|\btaipei\b|china.{0,20}(?:invade|blockade).{0,20}taiwan"),
    ("china_trade", r"china.{0,30}(?:trade|tariff|export|chip|semiconductor)|(?:trade deal|tariff).{0,30}china"),
    ("iran_hormuz", r"\biran\b|\bhormuz\b|persian gulf|kharg|farsi island|hengam|abu musa|tunb|houthi"),
    ("israel_middle_east", r"\bisrael\b|\bgaza\b|\bhamas\b|\bhezbollah\b|\blebanon\b|\bsyria\b|\bsaudi\b|middle east"),
    ("russia_ukraine", r"\brussia\b|\bukraine\b|\bnato\b|\bputin\b|zelensky|crimea|donbas|kremlin"),
    ("korea", r"north korea|south korea|kim jong"),
    ("rates", r"\bfed\b|federal reserve|rate cut|interest rate|\bfomc\b|treasury yield|fed funds"),
    ("recession", r"recession|\bgdp\b|unemployment|economic contraction|soft landing|jobs report|nonfarm|\bcpi\b|\bpce\b"),
    ("crypto", r"bitcoin|crypto|ethereum|binance|coinbase"),
    ("trade", r"tariff|trade deal|trade agreement|customs|\bimport\b|\bexport\b"),
    ("energy", r"\boil\b|\bgas\b|diesel|pipeline|\bopec\b|energy|refinery|shipping|strait"),
    ("defense", r"military|\bwar\b|strike|invasion|nuclear|missile|clash|attack"),
]


def classify_theme(text):
    s=(text or "").lower()
    for theme,pattern in THEME_PATTERNS:
        if re.search(pattern,s): return theme
    return "other"


def event_severity(title):
    s=(title or "").lower()
    if any(x in s for x in ("military clash","invasion","invade","blockade","strike","attack","war "," war","nuclear test","article 5","ground offensive","troops fighting","missile","forces in")):
        return 1.0
    if any(x in s for x in ("hormuz","sanction","tariff","export restriction","trade war","pipeline","oil supply","shipping")):
        return .85
    if any(x in s for x in ("ceasefire","peace deal","normalize relations","disarm","diplomatic","trade deal")):
        return .55
    if any(x in s for x in ("local election","mayoral","mayor ","party winner")):
        return .12
    if any(x in s for x in ("election","president out","out as president","leader out","meet with","shake hands","visit")):
        return .30
    return .65


def score_relevance(theme, sector, industry, ticker, title):
    sector=(sector or "").lower(); industry=(industry or "").lower(); ticker=ticker.upper(); title=(title or "").lower()
    sev=event_severity(title)
    score, sign, why = 0.05, None, "Broad event exposure"
    if theme == "taiwan":
        if "semiconductor" in industry or ticker in {"NVDA","AMD","AVGO","INTC","TSM","ASML","SOXX"}: score,sign,why=.98*sev,-1,"Semiconductor supply-chain exposure to Taiwan"
        elif sector in {"technology","consumer cyclical"}: score,sign,why=.72*sev,-1,"Asian electronics and supply-chain exposure"
    elif theme == "china_trade":
        if sector in {"technology","consumer cyclical","industrials"}: score,sign,why=.82*sev,-1,"China trade/export-policy exposure"
        if ticker in {"NVDA","AMD","AAPL","TSM","ASML","TSLA","SOXX"}: score=max(score,.92*sev); sign=-1; why="China semiconductor/trade-policy exposure"
    elif theme in {"iran_hormuz","israel_middle_east","energy"}:
        if sector == "energy" or ticker in {"XOM","CVX","COP","OXY","XLE"}: score,sign,why=.92*sev,1,"Oil supply and Middle East energy exposure"
        elif "airline" in industry or ticker in {"DAL","UAL","CCL"}: score,sign,why=.82*sev,-1,"Fuel-cost and travel-route exposure"
        elif sector in {"industrials","technology"}: score,sign,why=.38*sev,None,"Global risk and supply-chain exposure"
    elif theme in {"russia_ukraine","defense"}:
        if "aerospace" in industry or ticker in {"LMT","RTX","NOC","BA","ITA"}: score,sign,why=.90*sev,None,"Defense-spending and conflict exposure"
        elif sector == "energy": score,sign,why=.62*sev,None,"European energy-supply exposure"
        else: score,sign,why=.30*sev,None,"Global geopolitical risk exposure"
    elif theme == "trade":
        if sector in {"technology","consumer cyclical","industrials"}: score,sign,why=.70*sev,-1,"Trade-policy and cross-border supply-chain exposure"
    elif theme == "rates":
        if sector in {"financial services","technology"} or ticker in {"SPY","QQQ","XLF"}: score,sign,why=.72,None,"Interest-rate sensitivity"
    elif theme == "recession":
        score,sign,why=.68,-1,"Broad macroeconomic sensitivity"
        if sector in {"consumer cyclical","financial services","industrials"}: score=.82
    elif theme == "crypto":
        if ticker in {"COIN","MSTR"} or "crypto" in industry or "bitcoin" in industry: score,sign,why=.95*sev,1,"Direct crypto-market exposure"
        else: score=.08
    elif any(x in title for x in ("united states", "u.s.", "us ", "china", "global")):
        score=.18*sev
    return score, sign, why
