import { mount } from 'svelte';
import './app.css';
import App from './App.svelte';

const target = document.getElementById('app');

if (!target) {
  throw new Error('Failed to mount monitor UI: expected mount node "#app" in the DOM.');
}

mount(App, {
  target,
});
