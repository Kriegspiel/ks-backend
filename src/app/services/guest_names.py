from __future__ import annotations

# Curated from public chess-player name lists such as FIDE rankings and
# grandmaster lists. The generator combines these as guest_first_last.
GUEST_FIRST_NAMES = tuple(
    """
    adolf akiba alexander alexandra alexei
    alexey alexis alireza anatoly andras
    anish anna anton antuan arjun
    arthur aryam abhimanyu avital axel
    bassem bela benjamin bernd boris
    bruce curt carl carlos cecil
    christian christopher claude conel daniel
    david dawid darmen daryl denis
    ding dommaraju dorian dragoljub dusko
    edgar eduard efim emanuel emory
    eric ernst ertugrul eugenio evgeny
    fabiano fedor ferenc fidel florin
    francisco garry gata georg georgi
    georgy gideon gilberto giovanni gregory
    grigory gukesh harikrishna hikaru humpy
    ian igor ilya ivan jan
    javokhir jeffery joel jonathan jose
    joshua judit jules julio karsten
    kateryna keti kirill klaus konstantin
    krishnan larry laurent leinier levon
    ludek luke maia mairbek marc
    marcel maria marie mark maxime
    michael miguel mikhail milan milos
    mircea nancy nana natalia niaz
    nigel nikita nino nodirbek olga
    oscar pablo paul pentala peter
    pia qiyu radoslaw rafael rainer
    rameshbabu rashid ray robby robert
    roman ruy sally samuel sandro
    sarasadat savielly sebastien sergey shahriyar
    shakhriyar shamsiddin shen simen soso
    stanislav tania teimour tigran vasyl
    veselin victor viktor vincent vladimir
    vladislav vugar wang wesley wei
    wilhelm william wolfgang xie yasser
    yelena yifan yuri zoltan zsuzsa
    leela nurgyul tan emre saleh
    vidit raunak nihal maurice aravind
    koneru irina valentina ekaterina xu
    ju wenjun zhu zhongyi andrei
    """.split()
)

GUEST_LAST_NAMES = tuple(
    """
    adams alekhine andersen anand aronian
    assaubayeva bacrot bareev benko berkes
    blackburne bogoljubow boleslavsky botvinnik bronstein
    bruvzon capablanca carlsen caruana cheparinov
    chigorin christiansen cochrane costeniuc crouch
    dreev dubov duda dzagnidze euwe
    fischer firouzja fine gelfand georgiev
    giri gligoric grischuk guimard gukesh
    harikrishna hort hou howell ivanchuk
    jakovenko jussupow karjakin kamsky karpov
    kasparov kavalek king korchnoi kramnik
    krush larsen leko lilienthal lputian
    mamedov mamedyarov maroczy marshall matlakov
    meier miles morphy muzychuk nakamura
    navara nepomniachtchi narayanan nihal nimzowitsch
    nunn olafsson petrosian polgar ponce
    portisch praggnanandhaa radjabov rapport ribli
    rozentalis rubinstein sakayev salov sargissian
    seirawan shankland shirov short smyslov
    sokolov spassky steinitz suetin svidler
    tal tarrasch timman topalov turov
    vidmar vojtaszek wang wei xie
    ye yifan zhao zhu zhongyi
    zhukova ziatdinov zubov zuckertort zvjaginsev
    abrahams akobian artemiev azmaiparashvili bardeleben
    baturinsky beliavsky berliner bisguier blumenfeld
    bologan bocharov breyer byrne canal
    charousek colle damljanovic delchev dominguez
    eljanov erigaisi esipenko fedoseev filip
    gashimov glek goldin huebner iordachescu
    jobava kovacevic kovalyov kozul kupreichik
    quang chao liren liang malakhov
    malaniuk milov mista motylev movsesian
    naiditsch nikolic onischuk panno paravyan
    persson pomar potkin predke raznikov
    riazantsev rodshtein romanishin saric shabalov
    sokolovsky solozhenkin sutovsky tomashevsky trifunovic
    vanforeest vahap vitiugov volokitin yudasin
    yusupov zherebukh zhigalko simagin smirin
    so sveshnikov torre unzicker yermolinsky
    """.split()
)
