import sqlite3
from datetime import datetime

class DatabaseManager:
    """
    Класс для управления базой данных платной поликлиники.
    """

    def __init__(self, db_name="clinic.db"):
        """
        Инициализирует соединение с базой данных.

        Args:
            db_name: Имя файла базы данных.
        """
        self.db_name = db_name
        self.conn = None  # Инициализируем conn значением None
        self.cursor = None

    def connect(self):
        """Устанавливает соединение с базой данных."""
        try:
            self.conn = sqlite3.connect(self.db_name)
            self.cursor = self.conn.cursor()
            self.create_tables() # Убеждаемся, что таблицы созданы при подключении
        except sqlite3.Error as e:
            print(f"Ошибка подключения к базе данных: {e}")
            self.conn = None # Устанавливаем conn в None в случае ошибки
            self.cursor = None
            raise  # Re-raise exception для обработки выше

    def close(self):
        """Закрывает соединение с базой данных."""
        if self.conn:
            self.conn.close()
            self.conn = None
            self.cursor = None

    def create_tables(self):
        """
        Создает таблицы в базе данных, если они не существуют.
        """
        try:
            if not self.conn or not self.cursor:
                raise ValueError("Соединение с базой данных не установлено.")

            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS patients (
                    patient_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    first_name TEXT NOT NULL,
                    last_name TEXT NOT NULL,
                    date_of_birth TEXT,
                    phone_number TEXT,
                    email TEXT
                )
            """)

            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS doctors (
                    doctor_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    first_name TEXT NOT NULL,
                    last_name TEXT NOT NULL,
                    specialization TEXT NOT NULL,
                    phone_number TEXT,
                    email TEXT
                )
            """)

            self.cursor.execute("""
                CREATE TABLE IF NOT (doctor_id) REFERENCES doctors (doctor_id)
                )
            """)

            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS payments (
                    payment_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    appointment_id INTEGER NOT NULL,
                    payment_date TEXT NOT NULL,
                    amount REAL NOT NULL,
                    payment_method TEXT,
                    FOREIGN KEY (appointment_id) REFERENCES appointments (appointment_id)
                )
            """)

            self.conn.commit()
        except sqlite3.Error as e:
            print(f"Ошибка при создании таблиц: {e}")
            if self.conn:
                self.conn.rollback() # откатываем транзакцию при ошибке
            raise # Re-raise exception для обработки выше

    # Методы для работы с таблицей patients
    def add_patient(self, first_name, last_name, date_of_birth=None, phone_number=None, email=None):
        """Добавляет нового пациента в базу данных."""
        try:
            if not self.conn or not self.cursor:
                raise ValueError("Соединение с базой данных не установлено.")
            self.cursor.execute("""
                INSERT INTO patients (first_name, last_name, date_of_birth, phone_number, email)
                VALUES (?, ?, ?, ?, ?)
            """, (first_name, last_name, date_of_birth, phone_number, email))
            self.conn.commit()
            return self.cursor.lastrowid # Возвращаем ID добавленного пациента
        except sqlite3.Error as e:
            print(f"Ошибка при добавлении пациента: {e}")
            if self.conn:
                self.conn.rollback()
            return None # Возвращаем None в случае ошибки


    def get_patient(self, patient_id):
        """Получает информацию о пациенте по его ID."""
        try:
            if not self.conn or not self.cursor:
                raise ValueError("Соединение с базой данных не установлено.")

            self.cursor.execute("""
                SELECT patient_id, first_name, last_name, date_of_birth, phone_number, email
                FROM patients
                WHERE patient_id = ?
            """, (patient_id,))
            return self.cursor.fetchone()  # Возвращает кортеж с данными пациента или None, если не найден
        except sqlite3.Error as e:
            print(f"Ошибка при получении информации о пациенте: {e}")
            return None

    def update_patient(self, patient_id, first_name=None, last_name=None, date_of_birth=None, phone_number=None, email=None):
        """Обновляет информацию о пациенте."""
        try:
            if not self.conn or not self.cursor:
                raise ValueError("Соединение с базой данных не установлено.")

            # Создаем список для хранения обновляемых полей и их значений
            updates = []
            values = []

            if first_name is not None:
                updates.append("first_name = ?")
                values.append(first_name)
            if last_name is not None:
                updates.append("last_name = ?")
                values.append(last_name)
            if date_of_birth is not None:
                updates.append("date_of_birth = ?")
                values.append(date_of_birth)
            if phone_number is not None:
                updates.append("phone_number = ?")
                values.append(phone_number)
            if email is not None:
                updates.append("email = ?")
                values.append(email)

            # Если нет полей для обновления, выходим
            if not updates:
                return True  # Ничего не обновлено, но операция успешна

            # Формируем SQL-запрос
            sql = f"""
                UPDATE patients
                SET {', '.join(updates)}
                WHERE patient_id = ?
            """
            values.append(patient_id)

            # Выполняем запрос
            self.cursor.execute(sql, tuple(values))
            self.conn.commit()
            return True  # Обновление успешно
        except sqlite3.Error as e:
            print(f"Ошибка при обновлении информации о пациенте: {e}")
            if self.conn:
                self.conn.rollback()
            return False  # Обновление не удалось

    def delete_patient(self, patient_id):
        """Удаляет пациента из базы данных."""
        try:
            if not self.conn or not self.cursor:
                raise ValueError("Соединение с базой данных не установлено.")

            self.cursor.execute("""
                DELETE FROM patients
                WHERE patient_id = ?
            """, (patient_id,))
            self.conn.commit()
            return True
        except sqlite3.Error as e:
            print(f"Ошибка при удалении пациента: {e}")
            if self.conn:
                self.conn.rollback()
            return False

    def get_all_patients(self):
        """Возвращает список всех пациентов."""
        try:
            if not self.conn or not self.cursor:
                raise ValueError("Соединение с базой данных не установлено.")
            self.cursor.execute("SELECT * FROM patients")
            return self.cursor.fetchall()
        except sqlite3.Error as e:
            print(f"Ошибка при получении списка пациентов: {e}")
            return []

    # Методы для работы с таблицей doctors (аналогично patients)
    def add_doctor(self, first_name, last_name, specialization, phone_number=None, email=None):
        """Добавляет нового врача в базу данных."""
        try:
            if not self.conn or not self.cursor:
                raise ValueError("Соединение с базой данных не установлено.")

            self.cursor.execute("""
                INSERT INTO doctors (first_name, last_name, specialization, phone_number, email)
                VALUES (?, ?, ?, ?, ?)
            """, (first_name, last_name, specialization, phone_number, email))
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            print(f"Ошибка при добавлении врача: {e}")
            if self.conn:
                self.conn.rollback()
            return None

    def get_doctor(self, doctor_id):
        """Получает информацию о враче по его ID."""
        try:
            if not self.conn or not self.cursor:
                raise ValueError("Соединение с базой данных не установлено.")

            self.cursor.execute("""
                SELECT doctor_id, first_name, last_name, specialization, phone_number, email
                FROM doctors
                WHERE doctor_id = ?
            """, (doctor_id,))
            return self.cursor.fetchone()
        except sqlite3.Error as e:
            print(f"Ошибка при получении информации о враче: {e}")
            return None

    def update_doctor(self, doctor_id, first_name=None, last_name=None, specialization=None, phone_number=None, email=None):
        """Обновляет информацию о враче."""
        try:
            if not self.conn or not self.cursor:
                raise ValueError("Соединение с базой данных не установлено.")

            updates = []
            values = []

            if first_name is not None:
                updates.append("first_name = ?")
                values.append(first_name)
            if last_name is not None:
                updates.append("last_name = ?")
                values.append(last_name)
            if specialization is not None:
                updates.append("specialization = ?")
                values.append(specialization)
            if phone_number is not None:
                updates.append("phone_number = ?")
                values.append(phone_number)
            if email is not None:
                updates.append("email = ?")
                values.append(email)

            if not updates:
                return True

            sql = f"""
                UPDATE doctors
                SET {', '.join(updates)}
                WHERE doctor_id = ?
            """
            values.append(doctor_id)

            self.cursor.execute(sql, tuple(values))
            self.conn.commit()
            return True
        except sqlite3.Error as e:
            print(f"Ошибка при обновлении информации о враче: {e}")
            if self.conn:
                self.conn.rollback()
            return False

    def delete_doctor(self, doctor_id):
        """Удаляет врача из базы данных."""
        try:
            if not self.conn or not self.cursor:
                raise ValueError("Соединение с базой данных не установлено.")

            self.cursor.execute("""
                DELETE FROM doctors
                WHERE doctor_id = ?
            """, (doctor_id,))
            self.conn.commit()
            return True
        except sqlite3.Error as e:
            print(f"Ошибка при удалении врача: {e}")
            if self.conn:
                self.conn.rollback()
            return False

    def get_all_doctors(self):
        """Возвращает список всех врачей."""
        try:
            if not self.conn or not self.cursor:
                raise ValueError("Соединение с базой данных не установлено.")

            self.cursor.execute("SELECT * FROM doctors")
            return self.cursor.fetchall()
        except sqlite3.Error as e:
            print(f"Ошибка при получении списка врачей: {e}")
            return []

    # Методы для работы с таблицей appointments
    def add_appointment(self, patient_id, doctor_id, appointment_datetime, reason=None):
      """Добавляет новую запись на прием в базу данных."""
      try:
          if not self.conn or not self.cursor:
              raise ValueError("Соединение с базой данных не установлено.")

          # Валидация: убеждаемся, что patient_id и doctor_id существуют в соответствующих таблицах.
          if not self.get_patient(patient_id):
              raise ValueError(f"Пациент с ID {patient_id} не найден.")
          if not self.get_doctor(doctor_id):
              raise ValueError(f"Врач с ID {doctor_id} не найден.")

          # Валидация формата даты и времени
          try:
              datetime.fromisoformat(appointment_datetime)  # Пытаемся преобразовать строку в datetime
          except ValueError:
              raise ValueError("Неверный формат даты и времени. Используйте ISO формат (YYYY-MM-DD HH:MM:SS).")

          self.cursor.execute("""
              INSERT INTO appointments (patient_id, doctor_id, appointment_datetime, reason)
              VALUES (?, ?, ?, ?)
          """, (patient_id, doctor_id, appointment_datetime, reason))
          self.conn.commit()
          return self.cursor.lastrowid
      except sqlite3.Error as e:
          print(f"Ошибка при добавлении записи на прием: {e}")
          if self.conn:
              self.conn.rollback()
          return None
      except ValueError as e:
          print(f"Ошибка валидации: {e}")
          if self.conn:
              self.conn.rollback()
          return None


    def get_appointment(self, appointment_id):
        """Получает информацию о записи на прием по ее ID."""
        try:
            if not self.conn or not self.cursor:
                raise ValueError("Соединение с базой данных не установлено.")

            self.cursor.execute("""
                SELECT appointment_id, patient_id, doctor_id, appointment_datetime, reason
                FROM appointments
                WHERE appointment_id = ?
            """, (appointment_id,))
            return self.cursor.fetchone()
        except sqlite3.Error as e:
            print(f"Ошибка при получении информации о записи на прием: {e}")
            return None

    def update_appointment(self, appointment_id, patient_id=None, doctor_id=None, appointment_datetime=None, reason=None):
        """Обновляет информацию о записи на прием."""
        try:
            if not self.conn or not self.cursor:
                raise ValueError("Соединение с базой данных не установлено.")

            updates = []
            values = []

            if patient_id is not None:
                updates.append("patient_id = ?")
                values.append(patient_id)
            if doctor_id is not None:
                updates.append("doctor_id = ?")
                values.append(doctor_id)
            if appointment_datetime is not None:
                updates.append("appointment_datetime = ?")
                values.append(appointment_datetime)
            if reason is not None:
                updates.append("reason = ?")
                values.append(reason)

            if not updates:
                return True

            sql = f"""
                UPDATE appointments
                SET {', '.join(updates)}
                WHERE appointment_id = ?
            """
            values.append(appointment_id)

            self.cursor.execute(sql, tuple(values))
            self.conn.commit()
            return True
        except sqlite3.Error as e:
            print(f"Ошибка при обновлении информации о записи на прием: {e}")
            if self.conn:
                self.conn.rollback()
            return False

    def delete_appointment(self, appointment_id):
        """Удаляет запись на прием из базы данных."""
        try:
            if not self.conn or not self.cursor:
                raise ValueError("Соединение с базой данных не установлено.")

            self.cursor.execute("""
                DELETE FROM appointments
                WHERE appointment_id = ?
            """, (appointment_id,))
            self.conn.commit()
            return True
        except sqlite3.Error as e:
            print(f"Ошибка при удалении записи на прием: {e}")
            if self.conn:
                self.conn.rollback()
            return False

    def get_all_appointments(self):
        """Возвращает список всех записей на прием."""
        try:
            if not self.conn or not self.cursor:
                raise ValueError("Соединение с базой данных не установлено.")

            self.cursor.execute("SELECT * FROM appointments")
            return self.cursor.fetchall()
        except sqlite3.Error as e:
            print(f"Ошибка при получении списка записей на прием: {e}")
            return []

    def get_appointments_by_patient(self, patient_id):
        """Возвращает список записей на прием для конкретного пациента."""
        try:
            if not self.conn or not self.cursor:
                raise ValueError("Соединение с базой данных не установлено.")

            self.cursor.execute("""
                SELECT * FROM appointments
                WHERE patient_id = ?
            """, (patient_id,))
            return self.cursor.fetchall()
        except sqlite3.Error as e:
            print(f"Ошибка при получении записей на прием для пациента: {e}")
            return []

    def get_appointments_by_doctor(self, doctor_id):
        """Возвращает список записей на прием для конкретного врача."""
        try:
            if not self.conn or not self.cursor:
                raise ValueError("Соединение с базой данных не установлено.")

            self.cursor.execute("""
                SELECT * FROM appointments
                WHERE doctor_id = ?
            """, (doctor_id,))
            return self.cursor.fetchall()
        except sqlite3.Error as e:
            print(f"Ошибка при получении записей на прием для врача: {e}")
            return []

    # Методы для работы с таблицей payments
    def add_payment(self, appointment_id, payment_date, amount, payment_method=None):
        """Добавляет информацию об оплате."""
        try:
            if not self.conn or not self.cursor:
                raise ValueError("Соединение с базой данных не установлено.")

            # Валидация: убеждаемся, что appointment_id существует в таблице appointments.
            if not self.get_appointment(appointment_id):
                raise ValueError(f"Запись на прием с ID {appointment_id} не найдена.")

            # Валидация формата даты.
            try:
                datetime.fromisoformat(payment_date)
            except ValueError:
                raise ValueError("Неверный формат даты платежа. Используйте ISO формат (YYYY-MM-DD HH:MM:SS).")

            self.cursor.execute("""
                INSERT INTO payments (appointment_id, payment_date, amount, payment_method)
                VALUES (?, ?, ?, ?)
            """, (appointment_id, payment_date, amount, payment_method))
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            print(f"Ошибка при добавлении информации об оплате: {e}")
            if self.conn:
                self.conn.rollback()
            return None
        except ValueError as e:
            print(f"Ошибка валидации: {e}")
            if self.conn:
                self.conn.rollback()
            return None

    def get_payment(self, payment_id):
        """Получает информацию об оплате по ID."""
        try:
            if not self.conn or not self.cursor:
                raise ValueError("Соединение с базой данных не установлено.")

            self.cursor.execute("""
                SELECT payment_id, appointment_id, payment_date, amount, payment_method
                FROM payments
                WHERE payment_id = ?
            """, (payment_id,))
            return self.cursor.fetchone()
        except sqlite3.Error as e:
            print(f"Ошибка при получении информации об оплате: {e}")
            return None

    def update_payment(self, payment_id, appointment_id=None, payment_date=None, amount=None, payment_method=None):
        """Обновляет информацию об оплате."""
        try:
            if not self.conn or not self.cursor:
                raise ValueError("Соединение с базой данных не установлено.")

            updates = []
            values = []

            if appointment_id is not None:
                updates.append("appointment_id = ?")
                values.append(appointment_id)
            if payment_date is not None:
                updates.append("payment_date = ?")
                values.append(payment_date)
            if amount is not None:
                updates.append("amount = ?")
                values.append(amount)
            if payment_method is not None:
                updates.append("payment_method = ?")
                values.append(payment_method)

            if not updates:
                return True

            sql = f"""
                UPDATE payments
                SET {', '.join(updates)}
                WHERE payment_id = ?
            """
            values.append(payment_id)

            self.cursor.execute(sql, tuple(values))
            self.conn.commit()
            return True
        except sqlite3.Error as e:
            print(f"Ошибка при обновлении информации об оплате: {e}")
            if self.conn:
                self.conn.rollback()
            return False

    def delete_payment(self, payment_id):
        """Удаляет информацию об оплате."""
        try:
            if not self.conn or not self.cursor:
                raise ValueError("Соединение с базой данных не установлено.")

            self.cursor.execute("""
                DELETE FROM payments
                WHERE payment_id = ?
            """, (payment_id,))
            self.conn.commit()
            return True
        except sqlite3.Error as e:
            print(f"Ошибка при удалении информации об оплате: {e}")
            if self.conn:
                self.conn.rollback()
            return False

    def get_payments_by_appointment(self, appointment_id):
        """Возвращает список платежей для конкретной записи на прием."""
        try:
            if not self.conn or not self.cursor:
                raise ValueError("Соединение с базой данных не установлено.")

            self.cursor.execute("""
                SELECT * FROM payments
                WHERE appointment_id = ?
            """, (appointment_id,))
            return self.cursor.fetchall()
        except sqlite3.Error as e:
            print(f"Ошибка при получении платежей для записи на прием: {e}")
            return []

    def get_all_payments(self):
        """Возвращает список всех платежей."""
        try:
            if not self.conn or not self.cursor:
                raise ValueError("Соединение с базой данных не установлено.")

            self.cursor.execute("SELECT * FROM payments")
            return self.cursor.fetchall()
        except sqlite3.Error as e:
            print(f"Ошибка при получении списка платежей: {e}")
            return []


# Пример использования
if __name__ == '__main__':
    db_manager = DatabaseManager()
    try:
        db_manager.connect()

        # Добавление пациента
        patient_id = db_manager.add_patient("Иван", "Иванов", "1990-05-15", "8-912-345-67-89", "ivanov@example.com")
        if patient_id:
          print(f"Добавлен пациент с ID: {patient_id}")

        # Получение информации о пациенте
        patient = db_manager.get_patient(patient_id)
        if patient:
            print(f"Информация о пациенте: {patient}")

        # Добавление врача
        doctor_id = db_manager.add_doctor("Елена", "Петрова", "Терапевт", "8-923-456-78-90", "petrova@example.com")
        if doctor_id:
          print(f"Добавлен врач с ID: {doctor_id}")

        # Добавление записи на прием
        if patient_id and doctor_id:
            appointment_id = db_manager.add_appointment(patient_id, doctor_id, "2024-01-20 10:00:00", "Плановый осмотр")
            if appointment_id:
              print(f"Добавлена запись на прием с ID: {appointment_id}")

            
            if appointment_id:
                payment_id = db_manager.add_payment(appointment_id, "2024-01-20 10:05:00", 1500.00, "Карта")
                if payment_id:
                    print(f"Добавлена оплата с ID: {payment_id}")

            
            if patient_id:
                appointments = db_manager.get_appointments_by_patient(patient_id)
                print(f"Записи на прием для пациента {patient_id}: {appointments}")


    except Exception as e:
        print(f"Произошла ошибка: {e}") # Обрабатываем все исключения, возникшие внутри try-блока.
    finally:
        db_manager.close()
