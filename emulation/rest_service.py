from flask import Flask, jsonify
import random
import string
from datetime import datetime, timedelta

app = Flask(__name__)

class Company:
    def __init__(self):
        self.id = self.generate_id()
        self.name = self.generate_name()
        self.inn = self.generate_inn()
        self.size = self.generate_size()
        self.created_at = self.generate_created_at()
        self.company_type = self.generate_company_type()
        self.updated_at = datetime.now()

    def generate_id(self):
        # Генерация уникального id (например, от 1 до 1000 для простоты)
        return random.randint(1, 1000)

    def generate_name(self):
        # Генерация случайного названия компании
        adjectives = ["Alpha", "Beta", "Gamma", "Delta", "Epsilon", "Zeta", "Eta", "Theta", "Iota", "Kappa"]
        nouns = ["Solutions", "Technologies", "Industries", "Enterprises", "Designs", "Systems", "Services", "Holdings"]
        return f"{random.choice(adjectives)} {random.choice(nouns)}"

    def generate_inn(self):
        # Генерация случайного ИНН (до 100000)
        return random.randint(1, 99999)

    def generate_size(self):
        # Генерация размера компании
        return random.choice(['S', 'M', 'L'])

    def generate_created_at(self):
        # Генерация случайной даты для created_at
        start_date = datetime(2015, 1, 1)  # Начало периода
        end_date = datetime.now()  # Текущая дата
        random_date = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
        return random_date

    def generate_company_type(self):
        # Генерация случайного типа компании
        return random.choice(['IP', 'SELF', 'OOO'])

    def to_dict(self):
        # Преобразование в словарь для удобства
        return {
            "id": self.id,
            "name": self.name,
            "inn": self.inn,
            "size": self.size,
            "created_at": self.created_at,
            "company_type": self.company_type,
            "updated_at": self.updated_at,
        }

class Tariff:
    def __init__(self, id, name, description, price, start_at, ends_at, tariff_size):
        self.id = id
        self.name = name
        self.description = description
        self.price = price
        self.start_at = start_at
        self.ends_at = ends_at
        self.tariff_size = tariff_size
        self.updated_at = datetime.now()
        
    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "price": self.price,
            "start_at": self.start_at.isoformat(),
            "ends_at": self.ends_at.isoformat(),
            "tariff_size": self.tariff_size,
            "updated_at": self.updated_at.isoformat()
        }
    def __repr__(self):
        return f"Tariff(id={self.id}, name='{self.name}', price={self.price}, size={self.tariff_size})"
    
    @staticmethod
    def generate_random_string(length):
        """Генерация случайной строки заданной длины."""
        letters = string.ascii_letters + string.digits
        return ''.join(random.choice(letters) for _ in range(length))
    
    @classmethod
    def generate_random_tariff(cls):
        """Генерация случайного тарифа."""
        name = f"{cls.generate_random_string(6)} Plan"
        description = f"A random tariff plan for {name}."
        price = round(random.uniform(990, 99990), 2)  # Случайная цена между 990 и 99990
        start_at = datetime.now() + timedelta(days=random.randint(0, 365))  # Случайная дата начала в пределах года
        ends_at = start_at + timedelta(days=random.randint(30, 365))  # Случайная дата окончания через 30-365 дней
        tariff_size = random.randint(1, 6)  # Случайный размер тарифа от 1 до 6

        return cls(random.randint(1, 100000), name, description, price, start_at, ends_at, tariff_size)


class Seller:
    def __init__(self, id, name, position, phone, department_code, employed_at):
        self.id = id
        self.name = name
        self.position = position
        self.phone = phone
        self.department_code = department_code
        self.employed_at = employed_at

    def to_dict(self):
        """Преобразует объект в словарь для удобного отображения."""
        return {
            "id": self.id,
            "name": self.name,
            "position": self.position,
            "phone": self.phone,
            "department_code": self.department_code,
            "employed_at": self.employed_at,
        }

    @staticmethod
    def generate_random_string(length):
        """Генерация случайной строки заданной длины."""
        letters = string.ascii_letters
        return ''.join(random.choice(letters) for _ in range(length))
    
    @staticmethod
    def generate_random_phone():
        """Генерация случайного номера телефона."""
        return '+7' + ''.join(random.choices(string.digits, k=10))

    @classmethod
    def generate_random_seller(cls):
        """Генерация случайного продавца."""
        name = f"{cls.generate_random_string(5)} {cls.generate_random_string(7)}"
        positions = ['Manager', 'Sales Representative', 'Accountant', 'Developer', 'Marketing Specialist']
        position = random.choice(positions)
        phone = cls.generate_random_phone()
        department_codes = ['HR', 'Sales', 'Finance', 'IT', 'Marketing']
        department_code = random.choice(department_codes)
        employed_at = (datetime.now() - timedelta(days=random.randint(365, 730))).strftime('%Y-%m-%d %H:%M:%S')  # Случайная дата трудоустройства
        return cls(random.randint(1, 100000), name, position, phone, department_code, employed_at)
    
@app.route('/company', methods=['GET'])
def get_company():
    new_company = Company()
    return jsonify(new_company.to_dict())

@app.route('/seller', methods=['GET'])
def get_seller():
    seller = Seller.generate_random_seller()
    return jsonify(seller.to_dict())

@app.route('/tariff', methods=['GET'])
def get_tariff():
    new_tariff = Tariff.generate_random_tariff()
    return jsonify(new_tariff.to_dict())

if __name__ == '__main__':
    app.run(debug=False, port=8123)