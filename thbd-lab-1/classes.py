class CustomerBasket:
    def __init__(self):
        self.items = []
        self.quantities: []
        self.prices: []
        self.combos = {}

    def __repr__(self) -> str:
        out = "CustomerBasket:\n"
        if any([self.items is None, self.prices is None, self.quantities is None]):
            out += f"No items in the basket\n"
        else:
            for item, quantity, price in zip(self.items, self.quantities, self.prices):
                out += f"\t{item} x{quantity} at {price}\n"
        for combo in self.combos:
            out += f"\t{combo} with {self.combos[combo]}\n"
        return out


class UserClass:
    def __init__(self):
        self.name = ""
        self.surname = ""
        self.age = 0
        self.current_balance: 0.0
        self.basket = CustomerBasket()
        self.loyalty_programmes = {
            "BestCustomer": False,
            "MarchDiscountProgramme": True
        }
        self.very_complicated_list_of_dicts_of_lists = []

    def __repr__(self) -> str:
        out = f"{self.name} {self.surname}, {self.age} y.o., {self.current_balance}$\n"
        out += str(self.basket)
        out += "Loalty programmes:\n"
        for k, v in self.loyalty_programmes.items():
            out += f"\t{k}: {v}\n"
        out += "Very complicated list of dicts of lists:\n"
        for item in self.very_complicated_list_of_dicts_of_lists:
            out += str(item) + "\n"
        return out