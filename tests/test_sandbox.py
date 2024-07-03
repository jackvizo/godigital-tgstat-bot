import os
import unittest

from telethon import TelegramClient
from telethon.sessions import MemorySession

from globals import TEST_API_ID, TEST_API_HASH, TEST_TG_SERVER_IP

os.environ['ENV_FILE'] = '.env.test'


class TestTelethonClient(unittest.TestCase):
    def setUp(self):
        # Set up a mock client with a fake session
        client = TelegramClient('test', TEST_API_ID, TEST_API_HASH)
        client.session.set_dc(0, TEST_TG_SERVER_IP, 80)
        client.start(phone=phone, code_callback=lambda: code)

        self.client = TelegramClientMock(
            TelegramClient('test', 'api_id', 'api_hash', session=MemorySession())
        )

        # Patch the connect method to prevent real network connections
        self.client.connect = connection_mock

        # Start the client
        self.client.start()

    def tearDown(self):
        # Disconnect the client after tests
        self.client.disconnect()

    def test_send_message(self):
        # Mocking the send_message method to return a specific result
        self.client.send_message = unittest.mock.AsyncMock(return_value="Message sent")

        # Perform the send_message action
        result = self.client.send_message('username', 'Hello, this is a test message!')

        # Assert the message was sent
        self.assertEqual(result, "Message sent")

    def test_get_me(self):
        # Mocking the get_me method to return a fake user
        fake_user = {
            'id': 123456789,
            'username': 'testuser',
            'first_name': 'Test',
            'last_name': 'User'
        }
        self.client.get_me = unittest.mock.AsyncMock(return_value=fake_user)

        # Perform the get_me action
        user = self.client.get_me()

        # Assert the user details are correct
        self.assertEqual(user['id'], 123456789)
        self.assertEqual(user['username'], 'testuser')
        self.assertEqual(user['first_name'], 'Test')
        self.assertEqual(user['last_name'], 'User')


if __name__ == '__main__':
    unittest.main()
