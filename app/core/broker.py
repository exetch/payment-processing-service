"""Фабрика RabbitMQ-брокера."""

from faststream.rabbit import Channel, RabbitBroker


def build_broker(url: str) -> RabbitBroker:
    """Брокер, падающий на недоставленных сообщениях.

    ``on_return_raises`` обязателен: сообщения публикуются с ``mandatory=True``, но
    при дефолтном ``on_return_raises=False`` возврат недоставленного сообщения
    резолвит publish успехом. Relay пометил бы событие опубликованным, а оно
    не попало бы ни в одну очередь.
    """
    return RabbitBroker(url, default_channel=Channel(on_return_raises=True))
