#TODO - ch ange to a dataclass
class Boa:
    def __init__(self, plant_id, bid_offer_pair_id, accepted_price):
        self.plant_id = plant_id
        self.bid_offer_pair_id = bid_offer_pair_id
        self.accepted_price = accepted_price