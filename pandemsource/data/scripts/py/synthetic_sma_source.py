from datetime import datetime as dt
from datetime import timedelta as delta
import itertools
import random
import pandas as pd
import os
from pandemsource import util

signals = {
  "new-threat":{
    "start":dt(2023, 10, 2), 
    "end":dt(2024, 10, 30),
    "emotion":["fear", "anger", "surprise"],
    "sentiment":["negative", "", "", "", "", "positive"],
    "aspect":["contagion", "healthcare worker", "masks", "test", "flu", "pandemic"],
    "weight":100
  },
  "npi-bad":{
    "start":dt(2023, 10, 12),
    "end":dt(2024, 6, 30),
    "emotion":["anger", "sadness", "disgust", "trust", "anticipation"],
    "sentiment":["negative", "", "", "", "positive"],
    "aspect":["business", "school", "masks", "test", "govt", "care home"],
    "weight":60
  },
  "npi-good":{
    "start":dt(2024, 1, 1),
    "end":dt(2024, 6, 30),
    "emotion":["sadness", "trust", "anticipation"],
    "sentiment":["positive", "negative"],
    "aspect":["masks", "test", "govt", "care home"],
    "weight":60
  },
  "deads": {
    "start":dt(2023, 12, 7), 
    "end":dt(2024, 1, 15),
    "emotion":["fear", "sadness", "anger", "trust", "disgust"],
    "sentiment":["negative"],
    "aspect":["death rate", "cases", "healthcare worker","care home", "pandemic", "business"],
    "weight":80
  },
  "saturation":{
    "start": dt(2023, 10, 3),
    "end":dt(2024, 2, 1),
    "emotion":["fear", "sadness", "anger", "trust", "anticipation"],
    "sentiment":["negative", "", "", "", "positive"],
    "aspect":["patient", "care home", "school", "pandemic"],
    "weight":20
  },
  "vaccination-bad":{
    "start": dt(2023, 10, 3),
    "end":dt(2024, 2, 1),
    "emotion":["anger", "fear", "business", "trust", "surprise"],
    "sentiment":["negative","","","","positive" ],
    "aspect":["vaccines", "cases", "death-rate", "care home", "govt"],
    "weight":60
  },
  "vaccination-good":{
    "start": dt(2023, 12, 15),
    "end":dt(2024, 6, 30),
    "emotion":["trust", "anticipation", "anger", "joy", "business", "fear"],
    "sentiment":["positive", "", "", "", "negative"],
    "aspect":["vaccines", "care home", "govt"],
    "weight":40
  }
}

models = {
  "emotion":[
    "anger",
    "anticipation",
    "disgust",
    "fear",
    "joy",
    "sadness",
    "surprise",
    "trust"
  ],
  "sentiment":[
    "negative",
    "neutral",
    "positive"
  ],
  "aspect": {
    "masks":["FFP2", "buying masks", "using masks", "missing masks"],
    "vaccines":["secondary effects", "public health workers vaccins", "missing vaccins", "vaccination program"],
    "business":["job losses", "closures", "big pharma"],
    "school":["protect kids", "school closures", "school cluster", "learning issues"],
    "contagion":["infection risk", "food", "protect nature"],
    "pandemic":["covid-19", "borders", "community transmission"],
    "govt":["contact tracing", "pandemic managment", "communication"],
    "test":["missing tests", "promote testing", "test performance"],
    "death rate":["death toll", "morgues", "comorbidities"],
    "healthcare worker":["lack of resources", "saving lives", "overloaded system", "waiting"],
    "patient":["pain", "sypmtoms", "treatment", "risk factors"],
    "flu":["transmission", "prevention", "seasonal", "new strain"],
    "care home":["visits", "care home cluster", "care home prevention", "protect eldery"],
    "cases":["exponential cases", "fever", "coughing"],
  }
}

suggestions={
    "masks":{
       "FFP2":["Make ffp2 mandatory on public transport", "find a non plastic alternative to FFP2 masks", "force teachers to use FFP2"],
       "buying masks":["make masks free!!", "put a cap price on masks", "limit the number of masks sold", "allow home made masks at schools"],
          "using masks":["teach the people how to propely wear a mask", "stop forcing people to wear masks on open areas", "stop lying about masks"], 
          "missing masks":["make a decent mask stock during peace time", "ensure mask availability on pharmacies", "esure that public health workers have enough masks!!"]
       },
    "vaccines":{
      "secondary effects":["Ensure vaccines are not worst than the disease", "should not take a jab that gives you fever", "acknowledge that aluminium in jabs van kill"], 
      "public health workers vaccins":["ensure nurses and doctors are vaccinated", "Do not force doctors recovered from influenza to be vaccinated", "Give priority to public health workers"], 
      "missing vaccins":["Ensure the vaccine is present before the outbreak!!", "fund research not the war", "speed up vaccination delivery!!"], 
      "vaccination program":["Vaccinate elderly first!!", "stop explaining and start vaccinating!!","stop vaccinatig kids with experimental drugs!!", "stop playing with our ADN"]
    },
    "business":{
      "job losses":["provide fund support to restaurants", "stop killing business with unuseful measures", "ensure fired people can get their jobs back after all this ends"],
      "closures":["stop closing business just in case!", "allow people to party! stop closing clubs!", "ensure markets are keept open on sundays"], 
      "big pharma":["use their profits to fight climate change!", "open patents on drugs", "put the money back to public"]
    },
    "school":{
      "protect kids":["let kids see the face of their teachers", "test kids at shchol arrival", "prevent parents sending seek kids at school"], 
      "school closures":["should keep schools opened", "smaller classes will reduce the spread and then avoidn closure", "let the children learn!!!"], 
      "school cluster":["avoid spike in kids fever keeping them at home after Christmans", "close the school after more than two confirmed cases!!", "shouls do something before all the shcool get sea"], 
      "learning issues":["find a way to avoid kids having learning delays", "chante school program to recover the student level agter lockdown"]
    },
    "contagion":{
      "infection risk":["Kill the birds!!!!", "find and alternative to industry scale poultries", "farmers should use masks"], 
      "food":["Do not eat eggs!!!", "simple solution, get vegan!!", "if you smell chicken, just run!"], 
      "protect nature":["Should have listen before agressing nature", "stop keeping alive a system tha kill us and the planet"] 
    },
    "pandemic":{
      "covid-19":["leart from covid!", "do not repeat the same errors than covid", "if this is worst than covid, then prepare yourself"], 
      "borders":["close borders before this becoma a pandemics!!", "migrand birds does not know about borders", "allow war migrants to cross borders even on a pandemics"], 
      "community transmission":["find a way to stop the virus after spread into a community", "close high risk neighbors before is too late", "do not hesitate to discriminate before this entire thing get out of control"]
    },
    "govt":{
      "contact tracing":["give more human power to contact tracing", "doing contanct tracing propelry at first will save billions"], 
      "pandemic managment":["[USER] dhoulf stop thinking on reelection and start thinking on people health", "our authorities should learn to anticipate!!", "see how other countries have controled the spread"], 
      "communication":["give me a break with cases and deads!", "should better inform on measures if they want them to be respected", "tell the truth, accept errors, fight back the pandemic"]
    },
    "test":{
      "missing tests":["make sure tests are available for elderly", "do not make tests mandatories if they cannot be bought", "speed up test delivey"], 
      "promote testing":["please test yourself before visiting your loved ones", "implement rapid test on airports will save lives", "do you want to be a hero? do you have fever? test yourself!"], 
      "test performance":["make sure tests are reliable", "should not sell saliva tests since they are not reliable", "improve test performance"]
    },
    "death rate":{
      "death toll":["prepare the ICU now, cases are raising deaths will come", "please do something, silent deads are going on in nurse homes", "should learn from covid deads, increse hospital resources!!"], 
      "morgues":["should stop burocracy that is fueling morgue scandal", "should respect families and do not restrict access in morgyes", "should modernize morgues and crematoriums"], 
      "comorbidities":["you have diabetes? protect yorself! danger is close", "people at risk should be vaccinated first to avoid deads", "release comorbidity dead rates!"]
    },
    "healthcare worker":{
      "lack of resources":["stop closing beds!!", "ensuer ICU beds are operable, so they can save lives" , "want more nureses? make them eark €5k a month and 4 days a week, it will be must cheaper than another pandemics"], 
      "saving lives":["They should be prized for they awesome work, they are saving lives there!", "should allow nurses and doctors to do their job, save lives", "essential workers should be recognized as such in the hierarchy of our society, they save lives each day"], 
      "overloaded system":["should have leard from covid and it is happenind again! ICU are full!!", "should ensure ambulances can get there in time but gues what, they are all already in action", "should give a break to public health workers, they cannot be on constant war time!"], 
      "waiting":["should avoind long queues for patients at risk", "do not call an ambulance, it takes too long! go by your mean to urgencies", "should do something afteer 12 hours waiting to get a diagnostic... cough!"]
    },
    "patient":{
      "pain":["if you are having chest pain, do not hesitate, go to the doctor!", "do not send them home, patients are suffering!", "should stay at home with this headache, I hope is not the bird-flu!!"], 
      "sypmtoms":["if you have symptomps, get a test done!", "caughing on a public transport without a mask should ne a public offence", "Should admit that memory loses is a sencondary effect of the flu"], 
      "treatment":["drinking 10 liter of water a day gives you immunity to flu", "drink vodka! it is a better cure than vaccination", "paying $320 buck for a therapy should be forbidden"], 
      "risk factors":[]
    },
    "flu":{
      "transmission":["human animal transmission can be avoided by letting birds alone!", "should understand transmission before inforce measures", "someone should stop this flu before it becomes a pandemics!!!!"], 
      "prevention":["use masks to prevent infection! As simple as that", "follow the rules and stay home will stop the spread", "should speed up vaccination to prevent more deaths"], 
      "seasonal":["should keep calm, this is just another seasonal flu episode", "should stop worrying, just another flu", "should be careful this is too early to be the seasonal flu!"], 
      "new strain":["generalize NGS in order to tackle new straing before it is too late", "the new variant should have been stopped by prevention", "all measures should be reviewed with the new strain"]
    },
    "care home":{
      "visits":["should let me visit my parents isolation is worst than th flu", "you should be informed of risks before going for visit your granpa", "visits at nursing homes can be handled properly with social distancing rules"], 
      "care home cluster":["detect clusters at care home quicker!", "should isolate first cases on care home before the whole thing become a cluster", "adapt the care homes to allow people live outside will pervent clusters"], 
      "care home prevention":["Train the personal on nursing homes to prevnt infectiosn", "provie the right ventilation equipment to nursing home will avoid unnecessary infections", "national guidelines should be simplified to allow proper fiscalisation of nursing home condition for preventing flu spread"], 
      "protect eldery":["Our government should do something to ensure the elders are protected", "should ensure vaccin availability for elderly", "protect the elderly, keep kids at home"]
    },
    "cases":{
      "exponential cases":["Lock-down is te only solution to prevent exponential growth of cases", "follow the rules and the cases will go back agein", "doing nothing is the best solution to reach herd immunity"], 
      "fever":["you should not count as a case just because you have fever", "fever is the first of the symptomps, stay at home if you have it you may ne a case", "should treat te viros not the fever"], 
      "coughing":["better stay at home than caughing all day long rather than infecting collegues", "kids have always caugh! should let them go to school!", "if you hear coughing on a bus, get off it!"]
    }
  }

steps = [
  ["aspect"],
  ["aspect", "emotion"],
  ["aspect", "sentiment"]
]

countries = {
  "DE":100,
  "NL":60
}

lines = []
start = min(*[s["start"] for s in signals.values()])
end = max(*[s["end"] for s in signals.values()])
random.seed(a="pandem-2")
source = "sma-fx-2023"
pathogen = "J09.X"
# generating time series
for geo in countries:
  all_count = {}
  for step in steps:
    for combi in itertools.product(*[[(s,cat) for cat in  models[s]] for s in step]):
      key = {**{k:v for k,v in combi}, **{"geo_code":geo, "pathogen_code":pathogen, "source":source}}
      for d in [start + delta(days = i) for i in range(0, (end - start).days)]:
        daycount = 0
        for sn, s in signals.items():
          weight = s["weight"]
          half = int((s["end"]-s["start"]).days/2)
          maxday = s["start"] + delta(days = half)
          if d <= s["start"] or d >= s["end"]:
            weight = 0
          # adjusting weight based on matching elements being the first match the mos important
          attrweight = 1
          for var, value in key.items():
            if var in s:
              if value not in s[var]:
                attrweight = 0
              else:
                #if d == maxday:
                #  print(f"match found {var} {value} in {sn} max in {maxday}")
                tot = sum([len(s[var]) - s[var].index(i) for i in s[var] if i != ""])
                attrweight = attrweight * (len(s[var])- s[var].index(value))/tot 
          #print(f"w:{weight}, d:{d}, maxday:{maxday}, half:{half}, start:{s['start']}, end:{s['end']}")
          # adjusting weight based on position within the period
          weight = weight * attrweight * (1 - abs(d - maxday).days / half)
          daycount = daycount + random.randint(int(countries[geo] * weight*0.95), round(countries[geo]*weight*1.05))
        # at this point daycount contain the number of articles of the current days including all signals
        lines.append({**key, **{"date":d.strftime("%Y-%m-%d"), "article_count":daycount }})
        # calculating data for all topics
        if len(combi) == 1:
          all_count[d.strftime("%Y-%m-%d")] = (all_count.get(d.strftime("%Y-%m-%d")) or 0) + daycount
  for day, count in all_count.items():
    lines.append({**{"geo_code":geo, "pathogen_code":pathogen, "source":source}, **{"date":day, "article_count":count }})

df = pd.DataFrame(lines)
df.to_csv("sma_timeseries.csv", sep=',', encoding='utf-8')

def get_topic(pop, i):
  for t, f in pop:
    if i < f:
      return t
    else:
      i = i - f
  return None

#dealing with suggestions
sugg_dir = util.pandem_path("files", "nlp", "points", source)
if not os.path.exists(sugg_dir):
  os.makedirs(sugg_dir)
# deleting existing files
for f in os.listdir(sugg_dir):
  os.remove(os.path.join(sugg_dir, f))

# generating suggestion mining data
flat_sugs = {t:[{"sub_topic":st, "suggestion":s} for st in suggestions[t].keys() for s in suggestions[t][st]] for t in suggestions}
for geo in countries:
  counts = df[pd.isna(df["emotion"]) & pd.isna(df["sentiment"]) & (df["geo_code"] == geo)].groupby(["date", "aspect"]).sum("article_count")
  dates = [*counts.index.get_level_values(0).unique()]

  for d in dates:
    aspects_pop = sorted(tuple(counts.loc[d,].to_dict()["article_count"].items()), key = lambda p: -p[1])
    sumpop = sum(v for k, v in aspects_pop)
    if sumpop > 0:
      nsug = 35
      tops = [get_topic(aspects_pop, random.randint(0, sumpop-1)) for i in range(0, nsug)]
      topcounts = {t:len([tt for tt in tops if tt == t]) for t in set(tops)}
      sugges_by_topic = {t:[flat_sugs[t][i] for i in random.sample(range(0, len(flat_sugs[t])), min(wanted, len(flat_sugs[t])))] for t, wanted in topcounts.items()}
      suggs = [
        {
          **{"aspect":t, "source":source, "pathogen_code":pathogen, "reporting_period":f'{d} 00:00:00', "indicator":"article_count", "value":1, "geo_code":geo, "job":0, "stamp":0}, 
          **v
        } for t, vv in sugges_by_topic.items() for v in vv
      ]
      util.append_json(suggs, os.path.join(sugg_dir, f"{d}.json"))
    
